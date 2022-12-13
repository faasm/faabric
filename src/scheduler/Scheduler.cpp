#include <faabric/proto/faabric.pb.h>
#include <faabric/redis/Redis.h>
#include <faabric/scheduler/ExecutorFactory.h>
#include <faabric/scheduler/FunctionCallClient.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/snapshot/SnapshotClient.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/transport/PointToPointBroker.h>
#include <faabric/util/concurrent_map.h>
#include <faabric/util/environment.h>
#include <faabric/util/func.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>
#include <faabric/util/memory.h>
#include <faabric/util/network.h>
#include <faabric/util/random.h>
#include <faabric/util/scheduling.h>
#include <faabric/util/snapshot.h>
#include <faabric/util/string_tools.h>
#include <faabric/util/testing.h>
#include <faabric/util/timing.h>

#include <sys/eventfd.h>
#include <sys/file.h>
#include <sys/syscall.h>
#include <unordered_set>

#define FLUSH_TIMEOUT_MS 10000
#define GET_EXEC_GRAPH_SLEEP_MS 500
#define MAX_GET_EXEC_GRAPH_RETRIES 3

using namespace faabric::util;
using namespace faabric::snapshot;

namespace faabric::scheduler {

static faabric::util::ConcurrentMap<
  std::string,
  std::shared_ptr<faabric::scheduler::FunctionCallClient>>
  functionCallClients;

static faabric::util::
  ConcurrentMap<std::string, std::shared_ptr<faabric::snapshot::SnapshotClient>>
    snapshotClients;

MessageLocalResult::MessageLocalResult()
{
    eventFd = eventfd(0, EFD_CLOEXEC);
}

MessageLocalResult::~MessageLocalResult()
{
    if (eventFd >= 0) {
        close(eventFd);
    }
}

void MessageLocalResult::setValue(std::unique_ptr<faabric::Message>&& msg)
{
    this->promise.set_value(std::move(msg));
    eventfd_write(this->eventFd, (eventfd_t)1);
}

Scheduler& getScheduler()
{
    static Scheduler sch;
    return sch;
}

Scheduler::Scheduler()
  : thisHost(faabric::util::getSystemConfig().endpointHost)
  , conf(faabric::util::getSystemConfig())
  , reg(faabric::snapshot::getSnapshotRegistry())
  , broker(faabric::transport::getPointToPointBroker())
{
    // Set up the initial resources
    int cores = faabric::util::getUsableCores();
    thisHostResources.set_slots(cores);

    // Start the reaper thread
    reaperThread.start(conf.reaperIntervalSeconds);
}

Scheduler::~Scheduler()
{
    if (!_isShutdown) {
        SPDLOG_ERROR("Destructing scheduler without shutting down first");
    }
}

std::set<std::string> Scheduler::getAvailableHosts()
{
    redis::Redis& redis = redis::Redis::getQueue();
    return redis.smembers(AVAILABLE_HOST_SET);
}

void Scheduler::addHostToGlobalSet(const std::string& host)
{
    redis::Redis& redis = redis::Redis::getQueue();
    redis.sadd(AVAILABLE_HOST_SET, host);
}

void Scheduler::removeHostFromGlobalSet(const std::string& host)
{
    redis::Redis& redis = redis::Redis::getQueue();
    redis.srem(AVAILABLE_HOST_SET, host);
}

void Scheduler::addHostToGlobalSet()
{
    redis::Redis& redis = redis::Redis::getQueue();
    redis.sadd(AVAILABLE_HOST_SET, thisHost);
}

void Scheduler::resetThreadLocalCache()
{
    SPDLOG_DEBUG("Resetting scheduler thread-local cache");
}

void Scheduler::reset()
{
    SPDLOG_DEBUG("Resetting scheduler");
    resetThreadLocalCache();

    // Stop the function migration thread
    functionMigrationThread.stop();

    // Stop the reaper thread
    reaperThread.stop();

    // Shut down, then clear executors
    for (auto& ep : executors) {
        for (auto& e : ep.second) {
            e->shutdown();
        }
    }
    executors.clear();

    // Clear the point to point broker
    broker.clear();

    faabric::util::FullLock lock(mx);

    // Ensure host is set correctly
    thisHost = faabric::util::getSystemConfig().endpointHost;

    // Reset resources
    thisHostResources = faabric::HostResources();
    thisHostResources.set_slots(faabric::util::getUsableCores());

    // Reset scheduler state
    availableHostsCache.clear();
    registeredHosts.clear();
    threadResults.clear();
    threadResultMessages.clear();

    pushedSnapshotsMap.clear();

    // Reset function migration tracking
    inFlightRequests.clear();
    pendingMigrations.clear();

    // Records
    recordedMessagesAll.clear();
    recordedMessagesLocal.clear();
    recordedMessagesShared.clear();

    // Restart reaper thread
    reaperThread.start(conf.reaperIntervalSeconds);
}

void Scheduler::shutdown()
{
    reset();

    reaperThread.stop();

    removeHostFromGlobalSet(thisHost);

    _isShutdown = true;
}

void SchedulerReaperThread::doWork()
{
    getScheduler().reapStaleExecutors();
}

int Scheduler::reapStaleExecutors()
{
    faabric::util::FullLock lock(mx);

    if (executors.empty()) {
        SPDLOG_DEBUG("No executors to check for reaping");
        return 0;
    }

    std::vector<std::string> keysToRemove;

    int nReaped = 0;
    for (auto& execPair : executors) {
        std::string key = execPair.first;
        std::vector<std::shared_ptr<Executor>>& execs = execPair.second;
        std::vector<std::shared_ptr<Executor>> toRemove;

        if (execs.empty()) {
            continue;
        }

        SPDLOG_TRACE(
          "Checking {} executors for {} for reaping", execs.size(), key);

        faabric::Message& firstMsg = execs.back()->getBoundMessage();
        std::string user = firstMsg.user();
        std::string function = firstMsg.function();
        std::string masterHost = firstMsg.masterhost();

        for (auto exec : execs) {
            long millisSinceLastExec = exec->getMillisSinceLastExec();
            if (millisSinceLastExec < conf.boundTimeout) {
                // This executor has had an execution too recently
                SPDLOG_TRACE("Not reaping {}, last exec {}ms ago (limit {}ms)",
                             exec->id,
                             millisSinceLastExec,
                             conf.boundTimeout);
                continue;
            }

            // Check if executor is currently executing
            if (exec->isExecuting()) {
                SPDLOG_TRACE("Not reaping {}, currently executing", exec->id);
                continue;
            }

            SPDLOG_TRACE("Reaping {}, last exec {}ms ago (limit {}ms)",
                         exec->id,
                         millisSinceLastExec,
                         conf.boundTimeout);

            toRemove.emplace_back(exec);
            nReaped++;
        }

        // Remove those that need to be removed
        for (auto exec : toRemove) {
            // Shut down the executor
            exec->shutdown();

            // Remove and erase
            auto removed = std::remove(execs.begin(), execs.end(), exec);
            execs.erase(removed, execs.end());
        }

        // Unregister this host if no more executors remain on this host, and
        // it's not the master
        if (execs.empty()) {
            SPDLOG_TRACE("No remaining executors for {}", key);

            bool isMaster = thisHost == masterHost;
            if (!isMaster) {
                faabric::UnregisterRequest req;
                req.set_host(thisHost);
                req.set_user(user);
                req.set_function(function);

                getFunctionCallClient(masterHost)->unregister(req);
            }

            keysToRemove.emplace_back(key);
        }
    }

    // Remove and erase
    for (auto& key : keysToRemove) {
        SPDLOG_TRACE("Removing scheduler record for {}, no more executors",
                     key);
        executors.erase(key);
    }

    return nReaped;
}

long Scheduler::getFunctionExecutorCount(const faabric::Message& msg)
{
    faabric::util::SharedLock lock(mx);
    const std::string funcStr = faabric::util::funcToString(msg, false);
    return executors[funcStr].size();
}

int Scheduler::getFunctionRegisteredHostCount(const faabric::Message& msg)
{
    faabric::util::SharedLock lock(mx);
    return getFunctionRegisteredHosts(msg.user(), msg.function(), false).size();
}

const std::set<std::string>& Scheduler::getFunctionRegisteredHosts(
  const std::string& user,
  const std::string& func,
  bool acquireLock)
{
    faabric::util::SharedLock lock;
    if (acquireLock) {
        lock = faabric::util::SharedLock(mx);
    }
    std::string key = user + "/" + func;
    return registeredHosts[key];
}

void Scheduler::removeRegisteredHost(const std::string& host,
                                     const std::string& user,
                                     const std::string& function)
{
    faabric::util::FullLock lock(mx);
    std::string key = user + "/" + function;
    registeredHosts[key].erase(host);
}

void Scheduler::addRegisteredHost(const std::string& host,
                                  const std::string& user,
                                  const std::string& function)
{
    std::string key = user + "/" + function;
    registeredHosts[key].insert(host);
}

void Scheduler::vacateSlot()
{
    SPDLOG_INFO("[{}] Vacating one slot", thisHost);
    thisHostUsedSlots.fetch_sub(1, std::memory_order_acq_rel);
}

faabric::util::SchedulingDecision Scheduler::callFunctions(
  std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    // We assume all the messages are for the same function and have the
    // same master host
    faabric::Message& firstMsg = req->mutable_messages()->at(0);
    std::string masterHost = firstMsg.masterhost();

    // Get topology hint from message
    faabric::util::SchedulingTopologyHint topologyHint =
      firstMsg.topologyhint().empty()
        ? faabric::util::SchedulingTopologyHint::NONE
        : faabric::util::strToTopologyHint.at(firstMsg.topologyhint());

    bool isForceLocal =
      topologyHint == faabric::util::SchedulingTopologyHint::FORCE_LOCAL;

    // If we're not the master host, we need to forward the request back to the
    // master host. This will only happen if a nested batch execution happens.
    if (!isForceLocal && masterHost != thisHost) {
        std::string funcStr = faabric::util::funcToString(firstMsg, false);
        SPDLOG_DEBUG("Forwarding {} back to master {}", funcStr, masterHost);

        getFunctionCallClient(masterHost)->executeFunctions(req);
        SchedulingDecision decision(firstMsg.appid(), firstMsg.groupid());
        decision.returnHost = masterHost;
        return decision;
    }

    faabric::util::FullLock lock(mx);

    SchedulingDecision decision = doSchedulingDecision(req, topologyHint);

    // Pass decision as hint
    return doCallFunctions(req, decision, lock, topologyHint);
}

faabric::util::SchedulingDecision Scheduler::makeSchedulingDecision(
  std::shared_ptr<faabric::BatchExecuteRequest> req,
  faabric::util::SchedulingTopologyHint topologyHint)
{
    faabric::util::FullLock lock(mx);

    return doSchedulingDecision(req, topologyHint);
}

faabric::util::SchedulingDecision Scheduler::doSchedulingDecision(
  std::shared_ptr<faabric::BatchExecuteRequest> req,
  faabric::util::SchedulingTopologyHint topologyHint)
{
    int nMessages = req->messages_size();
    faabric::Message& firstMsg = req->mutable_messages()->at(0);
    std::string funcStr = faabric::util::funcToString(firstMsg, false);

    // If topology hints are disabled, unset the provided topology hint
    if (conf.noTopologyHints == "on" &&
        topologyHint != faabric::util::SchedulingTopologyHint::NONE) {
        SPDLOG_WARN("Ignoring topology hint passed to scheduler as hints are "
                    "disabled in the config");
        topologyHint = faabric::util::SchedulingTopologyHint::NONE;
    }

    // If requesting a cached decision, look for it now
    faabric::util::DecisionCache& decisionCache =
      faabric::util::getSchedulingDecisionCache();
    if (topologyHint == faabric::util::SchedulingTopologyHint::CACHED) {
        std::shared_ptr<faabric::util::CachedDecision> cachedDecision =
          decisionCache.getCachedDecision(req);

        if (cachedDecision != nullptr) {
            int groupId = cachedDecision->getGroupId();
            SPDLOG_DEBUG("Using cached decision for {} {}, group {}",
                         funcStr,
                         firstMsg.appid(),
                         groupId);

            // Get the cached hosts
            std::vector<std::string> hosts = cachedDecision->getHosts();

            // Create the scheduling decision
            faabric::util::SchedulingDecision decision(firstMsg.appid(),
                                                       groupId);
            for (int i = 0; i < hosts.size(); i++) {
                // Reuse the group id
                faabric::Message& m = req->mutable_messages()->at(i);
                m.set_groupid(groupId);
                m.set_groupsize(req->messages_size());

                // Add to the decision
                decision.addMessage(hosts.at(i), m);
            }

            return decision;
        }

        SPDLOG_DEBUG("No cached decision found for {} x {} in app {}",
                     req->messages_size(),
                     funcStr,
                     firstMsg.appid());
    }

    std::vector<std::string> hosts;
    hosts.reserve(nMessages);

    if (topologyHint == faabric::util::SchedulingTopologyHint::FORCE_LOCAL) {
        // We're forced to execute locally here so we do all the messages
        SPDLOG_TRACE("Scheduling {}/{} of {} locally (force local)",
                     nMessages,
                     nMessages,
                     funcStr);

        for (int i = 0; i < nMessages; i++) {
            hosts.push_back(thisHost);
        }
    } else {
        // At this point we know we're the master host, and we've not been
        // asked to force full local execution.

        // Work out how many we can handle locally
        int slots = thisHostResources.slots();
        if (topologyHint == faabric::util::SchedulingTopologyHint::UNDERFULL) {
            slots = slots / 2;
        }

        // Work out available cores, flooring at zero
        int available =
          slots - this->thisHostUsedSlots.load(std::memory_order_acquire);
        available = std::max<int>(available, 0);

        // Claim as many as we can
        int nLocally = std::min<int>(available, nMessages);

        // Add those that can be executed locally
        SPDLOG_TRACE(
          "Scheduling {}/{} of {} locally", nLocally, nMessages, funcStr);
        for (int i = 0; i < nLocally; i++) {
            hosts.push_back(thisHost);
        }

        // If some are left, we need to distribute.
        // First try and do so on already registered hosts.
        int remainder = nMessages - nLocally;
        if (remainder > 0) {
            const std::set<std::string>& thisRegisteredHosts =
              getFunctionRegisteredHosts(
                firstMsg.user(), firstMsg.function(), false);

            for (const auto& h : thisRegisteredHosts) {
                // Work out resources on the remote host
                faabric::HostResources r = getHostResources(h);
                int available = r.slots() - r.usedslots();

                // We need to floor at zero here in case the remote host is
                // overloaded, in which case its used slots will be greater than
                // its available slots.
                available = std::max<int>(0, available);
                int nOnThisHost = std::min<int>(available, remainder);

                // Under the NEVER_ALONE topology hint, we never choose a host
                // unless we can schedule at least two requests in it.
                if (topologyHint ==
                      faabric::util::SchedulingTopologyHint::NEVER_ALONE &&
                    nOnThisHost < 2) {
                    continue;
                }

                SPDLOG_TRACE("Scheduling {}/{} of {} on {} (registered)",
                             nOnThisHost,
                             nMessages,
                             funcStr,
                             h);

                for (int i = 0; i < nOnThisHost; i++) {
                    hosts.push_back(h);
                }

                remainder -= nOnThisHost;
                if (remainder <= 0) {
                    break;
                }
            }
        }

        // Now schedule to unregistered hosts if there are messages left
        if (remainder > 0) {
            std::vector<std::string> unregisteredHosts =
              getUnregisteredHosts(firstMsg.user(), firstMsg.function());

            for (const auto& h : unregisteredHosts) {
                // Skip if this host
                if (h == thisHost) {
                    continue;
                }

                // Work out resources on the remote host
                faabric::HostResources r = getHostResources(h);
                int available = r.slots() - r.usedslots();

                // We need to floor at zero here in case the remote host is
                // overloaded, in which case its used slots will be greater than
                // its available slots.
                available = std::max<int>(0, available);
                int nOnThisHost = std::min(available, remainder);

                if (topologyHint ==
                      faabric::util::SchedulingTopologyHint::NEVER_ALONE &&
                    nOnThisHost < 2) {
                    continue;
                }

                SPDLOG_TRACE("Scheduling {}/{} of {} on {} (unregistered)",
                             nOnThisHost,
                             nMessages,
                             funcStr,
                             h);

                // Register the host if it's exected a function
                if (nOnThisHost > 0) {
                    addRegisteredHost(h, firstMsg.user(), firstMsg.function());
                }

                for (int i = 0; i < nOnThisHost; i++) {
                    hosts.push_back(h);
                }

                remainder -= nOnThisHost;
                if (remainder <= 0) {
                    break;
                }
            }
        }

        // At this point there's no more capacity in the system, so we
        // just need to overload locally
        if (remainder > 0) {
            std::string overloadedHost = thisHost;

            // Under the NEVER_ALONE scheduling topology hint we want to
            // overload the last host we assigned requests to.
            if (topologyHint ==
                  faabric::util::SchedulingTopologyHint::NEVER_ALONE &&
                !hosts.empty()) {
                overloadedHost = hosts.back();
            }

            SPDLOG_TRACE("Overloading {}/{} {} {}",
                         remainder,
                         nMessages,
                         funcStr,
                         overloadedHost == thisHost
                           ? "locally"
                           : "to host " + overloadedHost);

            for (int i = 0; i < remainder; i++) {
                hosts.push_back(overloadedHost);
            }
        }
    }

    // Sanity check
    if (hosts.size() != nMessages) {
        SPDLOG_ERROR(
          "Serious scheduling error: {} != {}", hosts.size(), nMessages);

        throw std::runtime_error("Not enough scheduled hosts for messages");
    }

    // Set up decision
    SchedulingDecision decision(firstMsg.appid(), firstMsg.groupid());
    for (int i = 0; i < hosts.size(); i++) {
        decision.addMessage(hosts.at(i), req->messages().at(i));
    }

    // Cache decision for next time if necessary
    if (topologyHint == faabric::util::SchedulingTopologyHint::CACHED) {
        decisionCache.addCachedDecision(req, decision);
    }

    return decision;
}

faabric::util::SchedulingDecision Scheduler::callFunctions(
  std::shared_ptr<faabric::BatchExecuteRequest> req,
  faabric::util::SchedulingDecision& hint)
{
    faabric::util::FullLock lock(mx);
    return doCallFunctions(
      req, hint, lock, faabric::util::SchedulingTopologyHint::NONE);
}

faabric::util::SchedulingDecision Scheduler::doCallFunctions(
  std::shared_ptr<faabric::BatchExecuteRequest> req,
  faabric::util::SchedulingDecision& decision,
  faabric::util::FullLock& lock,
  faabric::util::SchedulingTopologyHint topologyHint)
{
    faabric::Message& firstMsg = req->mutable_messages()->at(0);
    std::string funcStr = faabric::util::funcToString(firstMsg, false);
    int nMessages = req->messages_size();
    bool isMain = thisHost == firstMsg.masterhost();
    bool isMigration = req->type() == faabric::BatchExecuteRequest::MIGRATION;
    // TODO - remove me
    if (isMigration) {
        SPDLOG_INFO("[{}] Migration call for {}:{}:{}",
                    thisHost,
                    firstMsg.appid(),
                    firstMsg.groupid(),
                    firstMsg.groupidx());
    }

    if (decision.hosts.size() != nMessages) {
        SPDLOG_ERROR(
          "Passed decision for {} with {} messages, but request has {}",
          funcStr,
          decision.hosts.size(),
          nMessages);
        throw std::runtime_error("Invalid scheduler hint for messages");
    }

    if (firstMsg.masterhost().empty()) {
        SPDLOG_ERROR("Request {} has no master host", funcStr);
        throw std::runtime_error("Message with no master host");
    }

    // Send out point-to-point mappings if necessary (unless being forced to
    // execute locally, in which case they will be transmitted from the
    // master)
    bool isForceLocal =
      topologyHint == faabric::util::SchedulingTopologyHint::FORCE_LOCAL;
    if (!isForceLocal && !isMigration && (firstMsg.groupid() > 0)) {
        if (firstMsg.ismpi()) {
            // If we are scheduling an MPI message, we want rank 0 to be in the
            // group. However, rank 0 is the one calling this method to schedule
            // the remaining worldSize - 1 functions. We can not change the
            // scheduling decision, as this would affect the downstream method,
            // but we can make a special copy just for the broker
            auto decisionCopy = decision;
            auto msgCopy = firstMsg;
            msgCopy.set_groupidx(0);
            decisionCopy.addMessage(thisHost, msgCopy);
            broker.setAndSendMappingsFromSchedulingDecision(decisionCopy);
            printSchedulingDecision(std::make_shared<faabric::util::SchedulingDecision>(decisionCopy));

            // For the moment, we only migrate MPI apps so it is safe to put
            // this check here
            if (firstMsg.migrationcheckperiod() > 0) {
                doStartFunctionMigrationThread(req);
            }
        } else {
            broker.setAndSendMappingsFromSchedulingDecision(decision);
        }
    }

    // We want to schedule things on this host _last_, otherwise functions may
    // start executing before all messages have been dispatched, thus slowing
    // the remaining scheduling. Therefore we want to create a list of unique
    // hosts, with this host last.
    std::vector<std::string> orderedHosts;
    bool isSingleHost = false;
    {
        std::set<std::string> uniqueHosts(decision.hosts.begin(),
                                          decision.hosts.end());
        bool hasFunctionsOnThisHost = uniqueHosts.contains(thisHost);

        // Mark the request as being single-host if necessary
        if (conf.noSingleHostOptimisations == 0) {
            std::set<std::string> thisHostUniset = { thisHost };
            isSingleHost = (uniqueHosts == thisHostUniset) && isMain;
            req->set_singlehost(isSingleHost);
        }

        if (hasFunctionsOnThisHost) {
            uniqueHosts.erase(thisHost);
        }

        orderedHosts = std::vector(uniqueHosts.begin(), uniqueHosts.end());

        if (hasFunctionsOnThisHost) {
            orderedHosts.push_back(thisHost);
        }
    }

    // -------------------------------------------
    // THREADS
    // -------------------------------------------
    bool isThreads = req->type() == faabric::BatchExecuteRequest::THREADS;

    // Register thread results if necessary
    if (isThreads) {
        for (const auto& m : req->messages()) {
            registerThread(m.id());
        }
    }

    // -------------------------------------------
    // SNAPSHOTS
    // -------------------------------------------

    // Push out snapshot diffs to registered hosts. We have to do this to
    // *all* hosts, regardless of whether they will be executing functions.
    // This greatly simplifies the reasoning about which hosts hold which
    // diffs.

    std::string snapshotKey;
    if (isThreads) {
        if (!firstMsg.snapshotkey().empty()) {
            SPDLOG_ERROR("{} should not provide snapshot key for {} threads",
                         funcStr,
                         req->messages().size());

            std::runtime_error("Should not provide snapshot key for threads");
        }

        if (!isSingleHost) {
            snapshotKey = faabric::util::getMainThreadSnapshotKey(firstMsg);
        }
    } else {
        snapshotKey = firstMsg.snapshotkey();
    }

    if (!snapshotKey.empty()) {
        auto snap = reg.getSnapshot(snapshotKey);

        for (const auto& host : getFunctionRegisteredHosts(
               firstMsg.user(), firstMsg.function(), false)) {
            std::shared_ptr<SnapshotClient> c = getSnapshotClient(host);

            // See if we've already pushed this snapshot to the given host,
            // if so, just push the diffs that have occurred in this main thread
            if (pushedSnapshotsMap[snapshotKey].contains(host)) {
                std::vector<faabric::util::SnapshotDiff> snapshotDiffs =
                  snap->getTrackedChanges();

                c->pushSnapshotUpdate(snapshotKey, snap, snapshotDiffs);
            } else {
                c->pushSnapshot(snapshotKey, snap);
                pushedSnapshotsMap[snapshotKey].insert(host);
            }
        }

        // Now reset the tracking on the snapshot before we start executing
        snap->clearTrackedChanges();
    } else if (!snapshotKey.empty() && isMigration && isForceLocal) {
        // If we are executing a migrated function, we don't need to distribute
        // the snapshot to other hosts, as this snapshot is specific to the
        // to-be-restored function
        auto snap = reg.getSnapshot(snapshotKey);

        // Now reset the tracking on the snapshot before we start executing
        snap->clearTrackedChanges();
    }

    // -------------------------------------------
    // EXECUTION
    // -------------------------------------------

    // Records for tests - copy messages before execution to avoid racing on msg
    size_t recordedMessagesOffset = recordedMessagesAll.size();
    if (faabric::util::isTestMode()) {
        for (int i = 0; i < nMessages; i++) {
            recordedMessagesAll.emplace_back(req->messages().at(i));
        }
    }

    // Iterate through unique hosts and dispatch messages
    for (const std::string& host : orderedHosts) {
        // Work out which indexes are scheduled on this host
        std::vector<int> thisHostIdxs;
        for (int i = 0; i < decision.hosts.size(); i++) {
            if (decision.hosts.at(i) == host) {
                thisHostIdxs.push_back(i);
            }
        }

        if (host == thisHost) {
            // -------------------------------------------
            // LOCAL EXECTUION
            // -------------------------------------------
            // For threads we only need one executor, for anything else we want
            // one Executor per function in flight.

            if (thisHostIdxs.empty()) {
                SPDLOG_DEBUG("Not scheduling any calls to {} out of {} locally",
                             funcStr,
                             nMessages);
                continue;
            }

            SPDLOG_DEBUG("Scheduling {}/{} calls to {} locally",
                         thisHostIdxs.size(),
                         nMessages,
                         funcStr);

            // Update slots
            this->thisHostUsedSlots.fetch_add(thisHostIdxs.size(),
                                              std::memory_order_acquire);

            if (isThreads) {
                // Threads use the existing executor. We assume there's only
                // one running at a time.
                std::vector<std::shared_ptr<Executor>>& thisExecutors =
                  executors[funcStr];

                std::shared_ptr<Executor> e = nullptr;
                if (thisExecutors.empty()) {
                    // Create executor if not exists
                    e = claimExecutor(firstMsg, lock);
                } else if (thisExecutors.size() == 1) {
                    // Use existing executor if exists
                    e = thisExecutors.back();
                } else {
                    SPDLOG_ERROR("Found {} executors for threaded function {}",
                                 thisExecutors.size(),
                                 funcStr);
                    throw std::runtime_error(
                      "Expected only one executor for threaded function");
                }

                assert(e != nullptr);

                // Execute the tasks
                e->executeTasks(thisHostIdxs, req);
            } else {
                // Non-threads require one executor per task
                for (auto i : thisHostIdxs) {
                    faabric::Message& localMsg = req->mutable_messages()->at(i);

                    if (localMsg.executeslocally()) {
                        faabric::util::UniqueLock resultsLock(
                          localResultsMutex);
                        localResults.insert(
                          { localMsg.id(),
                            std::make_shared<MessageLocalResult>() });
                    }

                    std::shared_ptr<Executor> e = claimExecutor(localMsg, lock);
                    e->executeTasks({ i }, req);
                }
            }
        } else {
            // -------------------------------------------
            // REMOTE EXECTUION
            // -------------------------------------------
            SPDLOG_DEBUG("Scheduling {}/{} calls to {} on {}",
                         thisHostIdxs.size(),
                         nMessages,
                         funcStr,
                         host);

            // Set up new request
            std::shared_ptr<faabric::BatchExecuteRequest> hostRequest =
              faabric::util::batchExecFactory();
            hostRequest->set_snapshotkey(req->snapshotkey());
            hostRequest->set_type(req->type());
            hostRequest->set_subtype(req->subtype());
            hostRequest->set_contextdata(req->contextdata());

            // Add messages
            for (auto msgIdx : thisHostIdxs) {
                auto* newMsg = hostRequest->add_messages();
                *newMsg = req->messages().at(msgIdx);
                newMsg->set_executeslocally(false);
            }

            // Dispatch the calls
            getFunctionCallClient(host)->executeFunctions(hostRequest);
        }
    }

    // Records for tests
    if (faabric::util::isTestMode()) {
        for (int i = 0; i < nMessages; i++) {
            std::string executedHost = decision.hosts.at(i);
            const faabric::Message& msg =
              recordedMessagesAll.at(recordedMessagesOffset + i);

            // Log results if in test mode
            if (executedHost.empty() || executedHost == thisHost) {
                recordedMessagesLocal.emplace_back(msg);
            } else {
                recordedMessagesShared.emplace_back(executedHost, msg);
            }
        }
    }

    return decision;
}

std::vector<std::string> Scheduler::getUnregisteredHosts(
  const std::string& user,
  const std::string& function,
  bool noCache)
{
    // Load the list of available hosts
    if (availableHostsCache.empty() || noCache) {
        availableHostsCache = getAvailableHosts();
    }

    // At this point we know we need to enlist unregistered hosts
    const std::set<std::string>& thisRegisteredHosts =
      getFunctionRegisteredHosts(user, function, false);

    std::vector<std::string> unregisteredHosts;

    std::set_difference(
      availableHostsCache.begin(),
      availableHostsCache.end(),
      thisRegisteredHosts.begin(),
      thisRegisteredHosts.end(),
      std::inserter(unregisteredHosts, unregisteredHosts.begin()));

    // If we've not got any, try again without caching
    if (unregisteredHosts.empty() && !noCache) {
        return getUnregisteredHosts(user, function, true);
    }

    return unregisteredHosts;
}

void Scheduler::broadcastSnapshotDelete(const faabric::Message& msg,
                                        const std::string& snapshotKey)
{
    const std::set<std::string>& thisRegisteredHosts =
      getFunctionRegisteredHosts(msg.user(), msg.function(), false);

    for (auto host : thisRegisteredHosts) {
        getSnapshotClient(host)->deleteSnapshot(snapshotKey);
    }
}

void Scheduler::callFunction(faabric::Message& msg, bool forceLocal)
{
    // TODO - avoid this copy
    auto req = faabric::util::batchExecFactory();
    *req->add_messages() = msg;

    // Specify that this is a normal function, not a thread
    req->set_type(req->FUNCTIONS);

    if (forceLocal) {
        req->mutable_messages()->at(0).set_topologyhint("FORCE_LOCAL");
    }

    // Make the call
    callFunctions(req);
}

void Scheduler::clearRecordedMessages()
{
    faabric::util::FullLock lock(mx);
    recordedMessagesAll.clear();
    recordedMessagesLocal.clear();
    recordedMessagesShared.clear();
}

std::vector<faabric::Message> Scheduler::getRecordedMessagesAll()
{
    faabric::util::SharedLock lock(mx);
    return recordedMessagesAll;
}

std::vector<faabric::Message> Scheduler::getRecordedMessagesLocal()
{
    faabric::util::SharedLock lock(mx);
    return recordedMessagesLocal;
}

std::shared_ptr<FunctionCallClient> Scheduler::getFunctionCallClient(
  const std::string& otherHost)
{
    auto client = functionCallClients.get(otherHost).value_or(nullptr);
    if (client == nullptr) {
        SPDLOG_DEBUG("Adding new function call client for {}", otherHost);
        client =
          functionCallClients.tryEmplaceShared(otherHost, otherHost).second;
    }
    return client;
}

std::shared_ptr<SnapshotClient> Scheduler::getSnapshotClient(
  const std::string& otherHost)
{
    auto client = snapshotClients.get(otherHost).value_or(nullptr);
    if (client == nullptr) {
        SPDLOG_DEBUG("Adding new snapshot client for {}", otherHost);
        client = snapshotClients.tryEmplaceShared(otherHost, otherHost).second;
    }
    return client;
}

std::vector<std::pair<std::string, faabric::Message>>
Scheduler::getRecordedMessagesShared()
{
    faabric::util::SharedLock lock(mx);
    return recordedMessagesShared;
}

std::shared_ptr<Executor> Scheduler::claimExecutor(
  faabric::Message& msg,
  faabric::util::FullLock& schedulerLock)
{
    std::string funcStr = faabric::util::funcToString(msg, false);

    std::vector<std::shared_ptr<Executor>>& thisExecutors = executors[funcStr];

    std::shared_ptr<faabric::scheduler::ExecutorFactory> factory =
      getExecutorFactory();

    std::shared_ptr<Executor> claimed = nullptr;
    for (auto& e : thisExecutors) {
        if (e->tryClaim()) {
            claimed = e;
            SPDLOG_DEBUG(
              "Reusing warm executor {} for {}", claimed->id, funcStr);
            break;
        }
    }

    // We have no warm executors available, so scale up
    if (claimed == nullptr) {
        SPDLOG_DEBUG("Scaling {} from {} -> {}",
                     funcStr,
                     thisExecutors.size(),
                     thisExecutors.size() + 1);

        // Spinning up a new executor can be lengthy, allow other things
        // to run in parallel
        schedulerLock.unlock();
        auto executor = factory->createExecutor(msg);
        schedulerLock.lock();
        thisExecutors.push_back(std::move(executor));
        claimed = thisExecutors.back();

        // Claim it
        claimed->tryClaim();
    }

    assert(claimed != nullptr);
    return claimed;
}

std::string Scheduler::getThisHost()
{
    faabric::util::SharedLock lock(mx);
    return thisHost;
}

void Scheduler::broadcastFlush()
{
    faabric::util::FullLock lock(mx);
    // Get all hosts
    std::set<std::string> allHosts = getAvailableHosts();

    // Remove this host from the set
    allHosts.erase(thisHost);

    // Dispatch flush message to all other hosts
    for (auto& otherHost : allHosts) {
        getFunctionCallClient(otherHost)->sendFlush();
    }

    lock.unlock();
    flushLocally();
}

void Scheduler::flushLocally()
{
    SPDLOG_INFO("Flushing host {}",
                faabric::util::getSystemConfig().endpointHost);

    // Reset this scheduler
    reset();

    // Flush the host
    getExecutorFactory()->flushHost();
}

void Scheduler::setFunctionResult(faabric::Message& msg)
{
    redis::Redis& redis = redis::Redis::getQueue();

    SPDLOG_INFO("[{}] Setting function result for {}:{}:{}", thisHost, msg.appid(), msg.groupid(), msg.groupidx());

    // Record which host did the execution
    msg.set_executedhost(faabric::util::getSystemConfig().endpointHost);

    // Set finish timestamp
    msg.set_finishtimestamp(faabric::util::getGlobalClock().epochMillis());

    // Remove the app from in-flight map if still there, and this host is the
    // master host for the message
    if (msg.masterhost() == thisHost) {
        removePendingMigration(msg.appid());
    }

    if (msg.executeslocally()) {
        faabric::util::UniqueLock resultsLock(localResultsMutex);

        auto it = localResults.find(msg.id());
        if (it != localResults.end()) {
            it->second->setValue(std::make_unique<faabric::Message>(msg));
        }

        // Sync messages can't have their results read twice, so skip
        // redis
        if (!msg.isasync()) {
            return;
        }
    }

    std::string key = msg.resultkey();
    if (key.empty()) {
        throw std::runtime_error("Result key empty. Cannot publish result");
    }

    // Remove the app from in-flight map if still there, and we are the leader
    // for the app
    // TODO - could we monitor then the request is done differently?
    if ((msg.migrationcheckperiod() > 0) && (msg.mpirank() == 0)) {
        SPDLOG_INFO("[{}] Start remove from in-flight {}:{}:{}", thisHost, msg.appid(), msg.groupid(), msg.groupidx());
        {
            // Acquire a lock to write on the in-flight-requests map
            faabric::util::FullLock lock(migrationThreadMx);
            inFlightRequests.erase(msg.appid());
        }

        // Now that the in-flight request is not in the map anymore, we can
        // safely remove the pending migration without a lock, as pending
        // migrations are only modified by reading the in-flight-requests first
        if (pendingMigrations.find(msg.appid()) != pendingMigrations.end()) {
            SPDLOG_INFO("[{}] Removing migration opportunity for app: {} (as it has finished)",
                            thisHost,
                            msg.appid());
            broadcastRemovePendingMigrations(pendingMigrations.at(msg.appid()));
            pendingMigrations.erase(msg.appid());
        }
        SPDLOG_INFO("[{}] Done remove from in-flight {}:{}:{}", thisHost, msg.appid(), msg.groupid(), msg.groupidx());
    }


    // Write the successful result to the result queue
    std::vector<uint8_t> inputData = faabric::util::messageToBytes(msg);
    redis.publishSchedulerResult(key, msg.statuskey(), inputData);
}

void Scheduler::registerThread(uint32_t msgId)
{
    // Here we need to ensure the promise is registered locally so
    // callers can start waiting
    threadResults[msgId];
}

void Scheduler::setThreadResult(
  const faabric::Message& msg,
  int32_t returnValue,
  const std::string& key,
  const std::vector<faabric::util::SnapshotDiff>& diffs)
{
    bool isMaster = msg.masterhost() == conf.endpointHost;
    if (isMaster) {
        if (!diffs.empty()) {
            // On master we queue the diffs locally directly, on a remote
            // host we push them back to master
            SPDLOG_DEBUG("Queueing {} diffs for {} to snapshot {} (group {})",
                         diffs.size(),
                         faabric::util::funcToString(msg, false),
                         key,
                         msg.groupid());

            auto snap = reg.getSnapshot(key);

            // Here we don't have ownership over all of the snapshot diff data,
            // but that's ok as the executor memory will outlast the snapshot
            // merging operation.
            snap->queueDiffs(diffs);
        }

        // Set thread result locally
        setThreadResultLocally(msg.id(), returnValue);
    } else {
        // Push thread result and diffs together
        getSnapshotClient(msg.masterhost())
          ->pushThreadResult(msg.id(), returnValue, key, diffs);
    }
}

void Scheduler::setThreadResultLocally(uint32_t msgId, int32_t returnValue)
{
    faabric::util::FullLock lock(mx);
    SPDLOG_DEBUG("Setting result for thread {} to {}", msgId, returnValue);
    threadResults.at(msgId).set_value(returnValue);
}

void Scheduler::setThreadResultLocally(uint32_t msgId,
                                       int32_t returnValue,
                                       faabric::transport::Message& message)
{
    setThreadResultLocally(msgId, returnValue);

    // Keep the message
    faabric::util::FullLock lock(mx);
    threadResultMessages.insert(std::make_pair(msgId, std::move(message)));
}

std::vector<std::pair<uint32_t, int32_t>> Scheduler::awaitThreadResults(
  std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    std::vector<std::pair<uint32_t, int32_t>> results;
    results.reserve(req->messages_size());
    for (int i = 0; i < req->messages_size(); i++) {
        uint32_t messageId = req->messages().at(i).id();

        int result = awaitThreadResult(messageId);
        results.emplace_back(messageId, result);
    }

    return results;
}

int32_t Scheduler::awaitThreadResult(uint32_t messageId)
{
    faabric::util::SharedLock lock(mx);
    auto it = threadResults.find(messageId);
    if (it == threadResults.end()) {
        SPDLOG_ERROR("Thread {} not registered on this host", messageId);
        throw std::runtime_error("Awaiting unregistered thread");
    }
    lock.unlock();

    return it->second.get_future().get();
}

void Scheduler::deregisterThreads(
  std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    faabric::util::FullLock eraseLock(mx);
    for (auto m : req->messages()) {
        threadResults.erase(m.id());
        threadResultMessages.erase(m.id());
    }
}

void Scheduler::deregisterThread(uint32_t msgId)
{
    // Erase the cached message and thread result
    faabric::util::FullLock eraseLock(mx);
    threadResults.erase(msgId);
    threadResultMessages.erase(msgId);
}

std::vector<uint32_t> Scheduler::getRegisteredThreads()
{
    faabric::util::SharedLock lock(mx);

    std::vector<uint32_t> registeredIds;
    for (auto const& p : threadResults) {
        registeredIds.push_back(p.first);
    }

    std::sort(registeredIds.begin(), registeredIds.end());

    return registeredIds;
}

size_t Scheduler::getCachedMessageCount()
{
    return threadResultMessages.size();
}

faabric::Message Scheduler::getFunctionResult(unsigned int messageId,
                                              int timeoutMs)
{
    bool isBlocking = timeoutMs > 0;

    if (messageId == 0) {
        throw std::runtime_error("Must provide non-zero message ID");
    }

    do {
        std::future<std::unique_ptr<faabric::Message>> fut;
        {
            faabric::util::UniqueLock resultsLock(localResultsMutex);
            auto it = localResults.find(messageId);
            if (it == localResults.end()) {
                break; // fallback to redis
            }
            fut = it->second->promise.get_future();
        }
        if (!isBlocking) {
            auto status = fut.wait_for(std::chrono::milliseconds(timeoutMs));
            if (status == std::future_status::timeout) {
                faabric::Message msgResult;
                msgResult.set_type(faabric::Message_MessageType_EMPTY);
                return msgResult;
            }
        } else {
            fut.wait();
        }

        {
            faabric::util::UniqueLock resultsLock(localResultsMutex);
            localResults.erase(messageId);
        }

        return *fut.get();
    } while (0);

    redis::Redis& redis = redis::Redis::getQueue();

    std::string resultKey = faabric::util::resultKeyFromMessageId(messageId);

    faabric::Message msgResult;

    if (isBlocking) {
        // Blocking version will throw an exception when timing out
        // which is handled by the caller.
        std::vector<uint8_t> result = redis.dequeueBytes(resultKey, timeoutMs);
        msgResult.ParseFromArray(result.data(), (int)result.size());
    } else {
        // Non-blocking version will tolerate empty responses, therefore
        // we handle the exception here
        std::vector<uint8_t> result;
        try {
            result = redis.dequeueBytes(resultKey, timeoutMs);
        } catch (redis::RedisNoResponseException& ex) {
            // Ok for no response when not blocking
        }

        if (result.empty()) {
            // Empty result has special type
            msgResult.set_type(faabric::Message_MessageType_EMPTY);
        } else {
            // Normal response if we get something from redis
            msgResult.ParseFromArray(result.data(), (int)result.size());
        }
    }

    return msgResult;
}

void Scheduler::getFunctionResultAsync(
  unsigned int messageId,
  int timeoutMs,
  asio::io_context& ioc,
  asio::any_io_executor& executor,
  std::function<void(faabric::Message&)> handler)
{
    if (messageId == 0) {
        throw std::runtime_error("Must provide non-zero message ID");
    }

    do {
        std::shared_ptr<MessageLocalResult> mlr;
        // Try to find matching local promise
        {
            faabric::util::UniqueLock resultsLock(localResultsMutex);
            auto it = localResults.find(messageId);
            if (it == localResults.end()) {
                break; // Fallback to redis
            }
            mlr = it->second;
        }
        // Asio wrapper for the MLR eventfd
        class MlrAwaiter : public std::enable_shared_from_this<MlrAwaiter>
        {
          public:
            unsigned int messageId;
            Scheduler* sched;
            std::shared_ptr<MessageLocalResult> mlr;
            asio::posix::stream_descriptor dsc;
            std::function<void(faabric::Message&)> handler;

            MlrAwaiter(unsigned int messageId,
                       Scheduler* sched,
                       std::shared_ptr<MessageLocalResult> mlr,
                       asio::posix::stream_descriptor dsc,
                       std::function<void(faabric::Message&)> handler)
              : messageId(messageId)
              , sched(sched)
              , mlr(std::move(mlr))
              , dsc(std::move(dsc))
              , handler(handler)
            {}

            ~MlrAwaiter()
            {
                // Ensure that Asio doesn't close the eventfd, to prevent a
                // double-close in the MLR destructor
                dsc.release();
            }

            void await(const boost::system::error_code& ec)
            {
                if (!ec) {
                    auto msg = mlr->promise.get_future().get();
                    handler(*msg);
                    {
                        faabric::util::UniqueLock resultsLock(
                          sched->localResultsMutex);
                        sched->localResults.erase(messageId);
                    }
                } else {
                    // The waiting task can spuriously wake up, requeue if this
                    // happens
                    doAwait();
                }
            }

            // Schedule this task waiting on the eventfd in the Asio queue
            void doAwait()
            {
                dsc.async_wait(asio::posix::stream_descriptor::wait_read,
                               beast::bind_front_handler(
                                 &MlrAwaiter::await, this->shared_from_this()));
            }
        };
        auto awaiter = std::make_shared<MlrAwaiter>(
          messageId,
          this,
          mlr,
          asio::posix::stream_descriptor(ioc, mlr->eventFd),
          std::move(handler));
        awaiter->doAwait();
        return;
    } while (0);

    // TODO: Use a non-blocking redis API here to avoid stalling the async
    // worker thread
    redis::Redis& redis = redis::Redis::getQueue();

    std::string resultKey = faabric::util::resultKeyFromMessageId(messageId);

    faabric::Message msgResult;

    // Blocking version will throw an exception when timing out
    // which is handled by the caller.
    std::vector<uint8_t> result = redis.dequeueBytes(resultKey, timeoutMs);
    msgResult.ParseFromArray(result.data(), (int)result.size());

    handler(msgResult);
}

faabric::HostResources Scheduler::getThisHostResources()
{
    // faabric::util::SharedLock lock(mx);
    // faabric::HostResources hostResources = thisHostResources;
    // Reset this every time instead of acquiring a lock
    faabric::HostResources hostResources = faabric::HostResources();
    hostResources.set_slots(faabric::util::getUsableCores());
    hostResources.set_usedslots(
      this->thisHostUsedSlots.load(std::memory_order_acquire));
    return hostResources;
}

void Scheduler::setThisHostResources(faabric::HostResources& res)
{
    faabric::util::FullLock lock(mx);
    thisHostResources = res;
    this->thisHostUsedSlots.store(res.usedslots(), std::memory_order_release);
}

faabric::HostResources Scheduler::getHostResources(const std::string& host)
{
    SPDLOG_TRACE("Requesting resources from {}", host);
    return getFunctionCallClient(host)->getResources();
}

// --------------------------------------------
// EXECUTION GRAPH
// --------------------------------------------

#define CHAINED_SET_PREFIX "chained_"
std::string getChainedKey(unsigned int msgId)
{
    return std::string(CHAINED_SET_PREFIX) + std::to_string(msgId);
}

void Scheduler::logChainedFunction(unsigned int parentMessageId,
                                   unsigned int chainedMessageId)
{
    redis::Redis& redis = redis::Redis::getQueue();

    const std::string& key = getChainedKey(parentMessageId);
    redis.sadd(key, std::to_string(chainedMessageId));
    redis.expire(key, STATUS_KEY_EXPIRY);
}

std::set<unsigned int> Scheduler::getChainedFunctions(unsigned int msgId)
{
    redis::Redis& redis = redis::Redis::getQueue();

    const std::string& key = getChainedKey(msgId);
    const std::set<std::string> chainedCalls = redis.smembers(key);

    std::set<unsigned int> chainedIds;
    for (auto i : chainedCalls) {
        chainedIds.insert(std::stoi(i));
    }

    return chainedIds;
}

ExecGraph Scheduler::getFunctionExecGraph(unsigned int messageId)
{
    ExecGraphNode rootNode = getFunctionExecGraphNode(messageId);
    ExecGraph graph{ .rootNode = rootNode };

    return graph;
}

ExecGraphNode Scheduler::getFunctionExecGraphNode(unsigned int messageId)
{
    redis::Redis& redis = redis::Redis::getQueue();

    // Get the result for this message
    std::string statusKey = faabric::util::statusKeyFromMessageId(messageId);

    // We want to make sure the message bytes have been populated by the time
    // we get them from Redis. For the time being, we retry a number of times
    // and fail if we don't succeed.
    std::vector<uint8_t> messageBytes = redis.get(statusKey);
    int numRetries = 0;
    while (messageBytes.empty() && numRetries < MAX_GET_EXEC_GRAPH_RETRIES) {
        SPDLOG_WARN(
          "Retry GET message for ExecGraph node with id {} (Retry {}/{})",
          messageId,
          numRetries + 1,
          MAX_GET_EXEC_GRAPH_RETRIES);
        SLEEP_MS(GET_EXEC_GRAPH_SLEEP_MS);
        messageBytes = redis.get(statusKey);
        ++numRetries;
    }
    if (messageBytes.empty()) {
        SPDLOG_ERROR("Can't GET message from redis (id: {}, key: {})",
                     messageId,
                     statusKey);
        throw std::runtime_error("Message for exec graph not in Redis");
    }

    faabric::Message result;
    result.ParseFromArray(messageBytes.data(), (int)messageBytes.size());

    // Recurse through chained calls
    std::set<unsigned int> chainedMsgIds = getChainedFunctions(messageId);
    std::vector<ExecGraphNode> children;
    for (auto c : chainedMsgIds) {
        children.emplace_back(getFunctionExecGraphNode(c));
    }

    // Build the node
    ExecGraphNode node{ .msg = result, .children = children };

    return node;
}

// ----------------------------------------
// MIGRATION
// ----------------------------------------

void FunctionMigrationThread::doWork()
{
    getScheduler().checkForMigrationOpportunities();
}

static bool arePendingMigrationsEqual(
    std::shared_ptr<faabric::PendingMigrations> pMigrationsA,
    std::shared_ptr<faabric::PendingMigrations> pMigrationsB)
{
    if (pMigrationsA->appid() != pMigrationsB->appid()) {
        return false;
    }
    if (pMigrationsA->groupid() != pMigrationsB->groupid()) {
        return false;
    }
    if (pMigrationsA->migrations_size() != pMigrationsB->migrations_size()) {
        return false;
    }
    for (int i = 0; i < pMigrationsA->migrations_size(); i++) {
        if (pMigrationsA->migrations().at(i).msg().mpirank()
          != pMigrationsB->migrations().at(i).msg().mpirank()) {
            return false;
        }
        if (pMigrationsA->migrations().at(i).srchost()
          != pMigrationsB->migrations().at(i).srchost()) {
            return false;
        }
        if (pMigrationsA->migrations().at(i).dsthost()
          != pMigrationsB->migrations().at(i).dsthost()) {
            return false;
        }
    }
    return true;
}

// We check for migration opportunities for all apps for which this host is the
// main host. Yet, all hosts executing a function in the app must be aware of
// the most up to date migration for it. This method periodically checks if
// the scheduling for the app could be improved according to a scheduling
// policy. At each check period, it will update the pending migrations it has
// detected and notify all hosts involved. A notification might be an update,
// or a removal
void Scheduler::checkForMigrationOpportunities()
{
    std::map<uint32_t, std::shared_ptr<faabric::PendingMigrations>>
      newPendingMigrations;
    typedef std::pair<uint32_t, std::shared_ptr<faabric::PendingMigrations>> AppPair;
    std::vector<AppPair> appIdsToRemove;
    std::vector<AppPair> appIdsToBroadcast;

    {
        // Acquire a shared lock to read from the in-flight requests map
        faabric::util::SharedLock lock(migrationThreadMx);

        // Check for migration opportunities of all in-flight requests (i.e.
        // requests for which this host is the main host)
        newPendingMigrations = doCheckForMigrationOpportunities();

        // Work out the diff between the old pending migrations and the new
        // ones
        auto newPendMigIt = newPendingMigrations.begin();
        auto oldPendMigIt = pendingMigrations.begin();
        while ((newPendMigIt != newPendingMigrations.end()) &&
          (oldPendMigIt != pendingMigrations.end())) {
            if (newPendMigIt->first == oldPendMigIt->first) {
                // If the same app is in the old and the new pending migrations
                // we check if both of them are the same. In this case we
                // must not remove the pending migration, as it is still valid,
                // but we can skip broadcasting it, as the other hosts are
                // already aware of it. If they are not the same, the migration
                // for the app has changed between check periods and we need
                // to _both_ remove the old one, and broadcast the new one
                if (arePendingMigrationsEqual(newPendMigIt->second,
                                              oldPendMigIt->second)) {
                    SPDLOG_INFO("[{}] Repeated migration opportunity for app: {}",
                                    thisHost,
                                    newPendMigIt->first);
                } else {
                    // We will overwrite the migration, we don't need to
                    // remove it
                    SPDLOG_INFO("Pushing back overwritten migration for app: {}",
                                newPendMigIt->first);
                    appIdsToBroadcast.emplace_back(
                      std::make_pair(newPendMigIt->first, newPendMigIt->second));
                }
                // Advance both iterators
                newPendMigIt++;
                oldPendMigIt++;
            } else if (newPendMigIt->first < oldPendMigIt->first) {
                // If we are here, it means that there is a new app in the
                // new set that is not in the old set, we want to broadcast
                // this one
                SPDLOG_INFO("Pushing back migration for app: {}",
                            newPendMigIt->first);
                appIdsToBroadcast.emplace_back(
                  std::make_pair(newPendMigIt->first, newPendMigIt->second));
                newPendMigIt++;
            } else {
                // If we are here, it means that there is an app in the old
                // set that it is not in the new set. This means that the
                // opportunity is stale if the app belongs to us (i.e. is part
                // of our in-flight requests)
                if (inFlightRequests.find(oldPendMigIt->first) != inFlightRequests.end()) {
                    appIdsToRemove.emplace_back(
                      std::make_pair(oldPendMigIt->first, oldPendMigIt->second));
                    oldPendMigIt++;
                }
            }
        }

        // Flush out the remaining arrays according to the same logic
        while (newPendMigIt != newPendingMigrations.end()) {
            // If we still have apps left in the new set, we want to broadcast
            // all of them
            SPDLOG_INFO("Pushing back migration for app: {}",
                        newPendMigIt->first);
            appIdsToBroadcast.emplace_back(
              std::make_pair(newPendMigIt->first, newPendMigIt->second));
            newPendMigIt++;
        }
        while (oldPendMigIt != pendingMigrations.end()) {
            // If we still have apps left in the old set, we want to remove
            // all of the ones that are also in our in-flight map
            if (inFlightRequests.find(oldPendMigIt->first) != inFlightRequests.end()) {
                appIdsToRemove.emplace_back(
                  std::make_pair(oldPendMigIt->first, oldPendMigIt->second));
                oldPendMigIt++;
            }
        }
    }

    {
        // Acquire full lock to write to the pending migrations map
        faabric::util::FullLock lock(migrationThreadMx);

        // Erase the pending migrations that are now stale
        for (const auto& [appId, msgPtr] : appIdsToRemove) {
            SPDLOG_INFO("[{}] Removing migration opportunity for app: {}",
                        thisHost,
                        appId);
            pendingMigrations.erase(appId);
        }

        // Broadcast the pending migrations that are new
        for (const auto& [appId, msgPtr] : appIdsToBroadcast) {
            SPDLOG_INFO("[{}] Broadcasting migration opportunity for app: {}",
                        thisHost,
                        appId);
            printMigration(msgPtr);
            pendingMigrations[appId] = std::move(msgPtr);
        }
    }

    // Lastly, send broadcast messages without any lock
    // TODO: potentially send less messages here (i.e. put together remove
    // and add)

    // Broadcast the deletion of the pending migrations that are now stale
    for (const auto& [appId, msgPtr] : appIdsToRemove) {
        broadcastRemovePendingMigrations(msgPtr);
    }

    // Broadcast the pending migrations that are new
    for (const auto& [appId, msgPtr] : appIdsToBroadcast) {
        broadcastPendingMigrations(msgPtr);
    }
}

void Scheduler::broadcastPendingMigrations(
  std::shared_ptr<faabric::PendingMigrations> pendingMigrations)
{
    // Get all hosts for the to-be migrated app
    auto msg = pendingMigrations->migrations().at(0).msg();
    const std::set<std::string>& thisRegisteredHosts =
      getFunctionRegisteredHosts(msg.user(), msg.function(), false);

    // Remove this host from the set
    registeredHosts.erase(thisHost);

    // Send pending migrations to all involved hosts
    for (const auto& otherHost : thisRegisteredHosts) {
        getFunctionCallClient(otherHost).sendPendingMigrations(
          pendingMigrations);
    }
}

void Scheduler::broadcastRemovePendingMigrations(
  std::shared_ptr<faabric::PendingMigrations> pendingMigrations)
{
    // Get all hosts for the to-be migrated app
    auto msg = pendingMigrations->migrations().at(0).msg();
    const std::set<std::string>& thisRegisteredHosts =
      getFunctionRegisteredHosts(msg.user(), msg.function(), false);

    // Remove this host from the set
    registeredHosts.erase(thisHost);

    // Send pending migrations to all involved hosts
    // TODO: actually we could just send the appid here
    for (const auto& otherHost : thisRegisteredHosts) {
        getFunctionCallClient(otherHost).sendRemovePendingMigrations(
          pendingMigrations);
    }
}

std::shared_ptr<faabric::PendingMigrations> Scheduler::getPendingAppMigrations(
  uint32_t appId, uint32_t groupId, uint32_t groupIdx)
{
    faabric::util::SharedLock lock(migrationThreadMx);

    if (pendingMigrations.find(appId) == pendingMigrations.end()) {
        return nullptr;
    }

    return pendingMigrations.at(appId);
}

/* TODO: this is a quite complex way to avoid a race condition where the
 * migration opportunity has been taken up by a new incoming request.
    auto group = faabric::transport::PointToPointGroup::getGroup(groupId);

    // If we are not the master index, wait for the master to work out if the
    // migration can take place or not
    if (groupIdx != POINT_TO_POINT_MASTER_IDX) {
        // Hit a barrier until the master finishes
        group->barrier(groupIdx);

        // Then check whether the migration can move forward or not
        faabric::util::SharedLock lock(mx);

        if (pendingMigrations.find(appId) == pendingMigrations.end()) {
            return nullptr;
        }

        return pendingMigrations.at(appId);
    }

    // If we are the master index, acquire a write lock to
    faabric::util::FullLock lock(mx);

    if (pendingMigrations.find(appId) == pendingMigrations.end()) {
        throw std::runtime_error(fmt::format("Can't find pending migration for app: {}", appId));
    }
    auto pMigration = pendingMigrations.at(appId);

    // For each host we are gonna migrate to, we send a reservation request
    // for as many functions we want to migrate. If the receiving host
    // accepts, then we are guaranteed that the slots will be free by the
    // time we migrate
    std::map<std::string, int> dstHostCount;
    for (int i = 0; i < pMigration->migrations_size(); i++) {
        auto m = pMigration->mutable_migrations()->at(i);
        // We just book one slot where we are migrating to, we don't vacate
        // the slot we occupy, this happens as part of the executor shutdown
        auto dstHost = m.dsthost();
        if (dstHostCount.find(dstHost) == dstHostCount.end()) {
            dstHostCount[dstHost] = 0;
        }
        dstHostCount.at(dstHost) += 1;
    }

    // TODO - fix the abortion so that it is not black and white
    for (auto& [dstHost, count] : dstHostCount) {
        faabric::ReserveSlotsRequest resReq;
        resReq.set_slots(count);
        bool success = false;
        if (dstHost == thisHost) {
            success = doReserveSlotsForPendingMigrations(count);
        } else {
            auto response = getFunctionCallClient(dstHost).sendReserveSlots(
              std::make_shared<faabric::ReserveSlotsRequest>(resReq));
            success = response.success();
        }
        if (!success) {
            // Tell other hosts involved to remove the pending migration
            broadcastRemovePendingMigrations(pMigration);
            // Remove the migration from our records
            inFlightRequests.erase(appId);
            pendingMigrations.erase(appId);
            pMigration = nullptr;
            break;
        }
    }

    group->barrier(POINT_TO_POINT_MASTER_IDX);
    return pMigration;
}
*/

void Scheduler::addPendingMigrations(
  std::shared_ptr<faabric::PendingMigrations> pMigration)
{
    faabric::util::FullLock lock(migrationThreadMx);

    SPDLOG_INFO("[{}] Received req. to add migration opportunity for app: {}", thisHost, pMigration->appid());
    pendingMigrations[pMigration->appid()] = std::move(pMigration);
}

void Scheduler::removePendingMigrations(uint32_t appId)
{
    faabric::util::FullLock lock(migrationThreadMx);

    SPDLOG_INFO("[{}] Received req. to remove migration opportunity for app: {}", thisHost, appId);
    if (inFlightRequests.find(appId) != inFlightRequests.end()) {
        SPDLOG_ERROR("Host {} should not have app {} in its in-flight requests!", thisHost, appId);
        throw std::runtime_error("Inconsistent in-flight map state!");
    }
    pendingMigrations.erase(appId);
}

std::map<uint32_t, std::shared_ptr<faabric::PendingMigrations>>
Scheduler::doCheckForMigrationOpportunities(
  faabric::util::MigrationStrategy migrationStrategy)
{
    std::map<uint32_t, std::shared_ptr<faabric::PendingMigrations>>
      newPendingMigrations;

    // For each in-flight request that has opted in to be migrated,
    // check if there is an opportunity to migrate
    for (const auto& [appId, req] : inFlightRequests) {
        // TODO - remove this sanity check
        if (req->messages_size() == 0) {
            SPDLOG_ERROR("whutt");
            throw std::runtime_error("make me an assertion plz");
        }
        auto& firstMsg = req->mutable_messages()->at(0);

        // At every migration check period we overwrite _all_ pending
        // migrations. Otherwise, pending migrations may become stale: the free
        // slots that they used can be taken up. Overwritting them is not a
        // catch-all neither, as slots can be occupied between migration checks

        faabric::PendingMigrations msg;
        msg.set_appid(firstMsg.appid());
        msg.set_groupid(firstMsg.groupid());

        if (migrationStrategy == faabric::util::MigrationStrategy::BIN_PACK) {
            // We sort the list of hosts assigned to this app by frequency. To
            // reduce fragmentation (i.e. BIN_PACK) we want to move functions
            // from hosts with lower frequency (right) to hosts with higher
            // frequency (left)
            // TODO: watch out, sorted hosts acquires a broker shared lock whilst
            // we have a migration thread shared lock here
            auto sortedHosts = broker.sortGroupHostsByFrequency(firstMsg.groupid());
            assert(sortedHosts.size() == req->messages_size());
            auto left = sortedHosts.begin();
            auto right = sortedHosts.end() - 1;
            // TODO: getThisHostResources acquires a shared lock on the
            // scheduler, whilst we hold a shared lock on the migration thread
            // here. Watch out for potential deadlocks
            faabric::HostResources r = (left->first == thisHost)
                                         ? getThisHostResources()
                                         : getHostResources(left->first);
            auto nAvailable = [&r]() -> int {
                return r.slots() - r.usedslots();
            };
            auto claimSlot = [&r]() {
                int currentUsedSlots = r.usedslots();
                r.set_usedslots(currentUsedSlots + 1);
            };
            while (left < right) {
                // If both pointers point to the same host, no migration
                // opportunity, and must check another possible source of
                // the migration
                if (left->first == right->first) {
                    --right;
                    continue;
                }

                // If the left pointer (possible destination of the
                // migration) is out of available resources, no migration
                // opportunity, and must check another possible destination
                // of migration
                if (nAvailable() == 0) {
                    auto oldHost = left->first;
                    ++left;
                    if (left->first != oldHost) {
                        r = (left->first == thisHost) ? getThisHostResources()
                                                : getHostResources(left->first);
                    }
                    continue;
                }

                if (right->second >= req->messages_size()) {
                    SPDLOG_ERROR("Index larger than number of messages: {} >= {}",
                                 right->second,
                                 req->messages_size());
                    throw std::runtime_error("turn me into an assertion plz");
                }
                faabric::Message* msgPtr = &(req->mutable_messages()->at(right->second));
                // TODO: make this an assertion
                if (msgPtr == nullptr) {
                    SPDLOG_ERROR("Something wrong with message ptr!");
                    throw std::runtime_error("Blehbleh");
                }
                if (msgPtr->groupidx() != right->second) {
                    SPDLOG_ERROR("Unexpected group idx: {} != {} (appid: {})",
                                 msgPtr->groupidx(),
                                 right->second,
                                 msgPtr->appid());
                    throw std::runtime_error("Blehbleh");
                }
                // TODO: support main function migration
                if (right->second == 0) {
                    SPDLOG_DEBUG("App {} avoiding migrating rank 0", right->second);
                    --right;
                    continue;
                }

                // If each pointer points to a request scheduled in a
                // different host, and the possible destination has slots,
                // there is a migration opportunity
                auto* migration = msg.add_migrations();
                migration->set_srchost(right->first);
                migration->set_dsthost(left->first);

                auto* migrationMsgPtr = migration->mutable_msg();
                *migrationMsgPtr = *msgPtr;
                // Decrement by one the availability, and check for more
                // possible sources of migration
                claimSlot();
                --right;
            }
        } else {
            SPDLOG_ERROR("Unrecognised migration strategy: {}",
                         migrationStrategy);
            throw std::runtime_error("Unrecognised migration strategy.");
        }

        if (msg.migrations_size() > 0) {
            newPendingMigrations[msg.appid()] =
              std::make_shared<faabric::PendingMigrations>(msg);
            SPDLOG_DEBUG("Detected migration opportunity for app: {} (gid: {})",
                         msg.appid(), msg.groupid());
        } else {
            SPDLOG_DEBUG("No migration opportunity detected for app: {}",
                         msg.appid());
        }
    }

    return newPendingMigrations;
}

// Start the function migration thread if necessary
// NOTE: ideally, instead of allowing the applications to specify a check
// period, we would have a default one (overwritable through an env.
// variable), and apps would just opt in/out of being migrated. We set
// the actual check period instead to ease with experiments.
// NOTE2: we are here only if req corresponds to the second message of an MPI
// application, see the place in the code where this method is called for
// more details
void Scheduler::doStartFunctionMigrationThread(
  std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    bool startMigrationThread = false;
    faabric::Message& firstMsg = req->mutable_messages()->at(0);

    // MPI applications are made up of two different requests: the original one
    // (with one message) and the second one (with world size - 1 messages)
    // created during world creation time. Thus, to correctly track migration
    // opportunities we must merge both. We create a deep copy of the original
    // request (rather than amending it) not to affect the rest of the
    // scheduling
    auto msgCopy = firstMsg;
    msgCopy.set_groupidx(0);
    auto reqCopy = std::make_shared<faabric::BatchExecuteRequest>();
    auto* msg = reqCopy->add_messages();
    *msg = msgCopy;
    for (int i = 0; i < firstMsg.mpiworldsize() - 1; i++) {
        auto* msg = reqCopy->add_messages();
        *msg = req->messages().at(i);
    }

    SPDLOG_INFO("[{}] Start add to in-flight {}:{}:{}", thisHost, firstMsg.appid(), firstMsg.groupid(), firstMsg.groupidx());
    {
        // Acquire the function migration thread lock to write into the in
        // flight requests map
        faabric::util::FullLock lock(migrationThreadMx);

        // Decide if we need to start the migration thread
        // TODO: could use an atomic bool instead
        startMigrationThread = inFlightRequests.empty();

        if (inFlightRequests.find(firstMsg.appid()) != inFlightRequests.end()) {
            SPDLOG_ERROR("There is already an in-flight request for app {}",
                         firstMsg.appid());
            throw std::runtime_error("App already in-flight");
        }

        inFlightRequests[firstMsg.appid()] = reqCopy;
    }
    SPDLOG_INFO("[{}] Done add to in-flight {}:{}:{}", thisHost, firstMsg.appid(), firstMsg.groupid(), firstMsg.groupidx());


    // Decide wether we have to start the migration thread or not
    if (startMigrationThread) {
        functionMigrationThread.start(firstMsg.migrationcheckperiod());
    } else if (firstMsg.migrationcheckperiod() !=
               functionMigrationThread.getIntervalSeconds()) {
        SPDLOG_WARN("Ignoring migration check period for app {} as the"
                    "migration thread is already running with a different"
                    " check period (provided: {}, current: {})",
                    firstMsg.appid(),
                    firstMsg.migrationcheckperiod(),
                    functionMigrationThread.getIntervalSeconds());
    }
}
}
