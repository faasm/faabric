#include <faabric/planner/PlannerClient.h>
#include <faabric/planner/planner.pb.h>
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

// Even though there's just one planner server, and thus there will only be
// one client per scheduler instance, using a ConcurrentMap gives us the
// thread-safe wrapper for free
static faabric::util::
  ConcurrentMap<std::string, std::shared_ptr<faabric::planner::PlannerClient>>
    plannerClient;

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
    auto availableHosts = getPlannerClient()->getAvailableHosts();
    std::set<std::string> availableHostsIps;
    for (const auto& host : availableHosts) {
        availableHostsIps.insert(host.ip());
    }

    return availableHostsIps;
}

void Scheduler::addHostToGlobalSet(
  const std::string& hostIp,
  std::shared_ptr<faabric::HostResources> overwriteResources)
{
    // Build register host request
    auto req = std::make_shared<faabric::planner::RegisterHostRequest>();
    req->mutable_host()->set_ip(hostIp);
    if (overwriteResources != nullptr) {
        req->mutable_host()->set_slots(overwriteResources->slots());
        req->mutable_host()->set_usedslots(overwriteResources->usedslots());
    } else if (hostIp == thisHost) {
        req->mutable_host()->set_slots(faabric::util::getUsableCores());
        req->mutable_host()->set_usedslots(0);
    }

    int plannerTimeout = getPlannerClient()->registerHost(req);

    // Once the host is registered, set-up a periodic thread to send a heart-
    // beat to the planner. Note that this method may be called multiple times
    // during the tests, so we only set the scheduler's variable if we are
    // actually registering this host. Also, only start the keep-alive thread
    // if not in test mode
    if (hostIp == thisHost && !faabric::util::isTestMode()) {
        keepAliveThread.setRequest(req);
        keepAliveThread.start(plannerTimeout / 2);
    }
}

void Scheduler::addHostToGlobalSet()
{
    addHostToGlobalSet(thisHost);
}

void Scheduler::removeHostFromGlobalSet(const std::string& hostIp)
{
    auto req = std::make_shared<faabric::planner::RemoveHostRequest>();
    bool isThisHost =
      hostIp == thisHost && keepAliveThread.thisHostReq != nullptr;
    if (isThisHost) {
        *req->mutable_host() = *keepAliveThread.thisHostReq->mutable_host();
    } else {
        req->mutable_host()->set_ip(hostIp);
    }

    getPlannerClient()->removeHost(req);

    // Clear the keep alive thread
    if (isThisHost) {
        keepAliveThread.stop();
    }
}

void Scheduler::resetThreadLocalCache()
{
    SPDLOG_DEBUG("Resetting scheduler thread-local cache");
}

void Scheduler::reset()
{
    SPDLOG_DEBUG("Resetting scheduler");
    resetThreadLocalCache();

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
    threadResults.clear();
    threadResultMessages.clear();

    pushedSnapshotsMap.clear();

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

            /* TODO: remove me
            bool isMaster = thisHost == masterHost;
            if (!isMaster) {
                faabric::UnregisterRequest req;
                req.set_host(thisHost);
                req.set_user(user);
                req.set_function(function);

                getFunctionCallClient(masterHost)->unregister(req);
            }
            */

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
    return getFunctionRegisteredHosts(msg).size();
}

const std::set<std::string> Scheduler::getFunctionRegisteredHosts(
  const faabric::Message& msg)
{
    return broker.getHostsRegisteredForGroup(msg.groupid());
}

void Scheduler::vacateSlot()
{
    thisHostUsedSlots.fetch_sub(1, std::memory_order_acq_rel);
}

// Special entrypoint to `just` execute functions in this host. Right now it is
// retro-fitted to the old scheduling behaviour. TODO: make it the default
// entrypoint
void Scheduler::executeBatchRequest(
  std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    faabric::Message& firstMsg = req->mutable_messages()->at(0);
    auto topologyHint = faabric::util::SchedulingTopologyHint::FORCE_LOCAL;

    // TODO: we only create this fake decision because the current
    // doCallFunctions API requires us to pass a SchedulingDecision (even if
    // the topology hint is FORCE_LOCAL)
    faabric::util::SchedulingDecision fakeDecision(firstMsg.appid(),
                                                   firstMsg.groupid());
    for (const auto& msg : req->messages()) {
        fakeDecision.addMessage(thisHost, msg);
    }

    // Do we even need this lock?
    faabric::util::FullLock lock(mx);

    doCallFunctions(req, fakeDecision, lock, topologyHint);
}

// This function is the entry point to trigger the scheduling of a batch of
// messages
faabric::util::SchedulingDecision Scheduler::callFunctions(
  std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    auto decision = getPlannerClient()->callFunctions(req);
    // The decision sets a group id for PTP communication. Make sure we
    // propagate the group id to the messages in the request. The group idx
    // is set when creating the request
    req->set_groupid(decision.groupId);
    for (auto& msg : *req->mutable_messages()) {
        msg.set_groupid(decision.groupId);
    }
    return decision;
    /*
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
    */
}

/*
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

            SPDLOG_DEBUG("Overloading {}/{} {} {}",
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
*/

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
    if (req->messages_size() == 0) {
        SPDLOG_ERROR("Got request to execute batch with 0 messages!");
        throw std::runtime_error("Malformed batch execute request");
    }
    faabric::Message& firstMsg = req->mutable_messages()->at(0);
    std::string funcStr = faabric::util::funcToString(firstMsg, false);
    int nMessages = req->messages_size();
    // TODO: does this work for threads too?
    bool isMain = firstMsg.groupidx() == 0;
    bool isMigration = req->type() == faabric::BatchExecuteRequest::MIGRATION;

    if (decision.hosts.size() != nMessages) {
        SPDLOG_ERROR(
          "Passed decision for {} with {} messages, but request has {}",
          funcStr,
          decision.hosts.size(),
          nMessages);
        throw std::runtime_error("Invalid scheduler hint for messages");
    }

    std::set<std::string> orderedHosts =
      broker.getHostsRegisteredForGroup(req->groupid());
    bool isSingleHost =
      orderedHosts.contains(thisHost) && (orderedHosts.size() == 1) && isMain;
    if (conf.noSingleHostOptimisations == 0) {
        std::set<std::string> thisHostUniset = { thisHost };
        req->set_singlehost(isSingleHost);
    }

    /*
    {
        std::set<std::string> uniqueHosts(decision.hosts.begin(),
                                          decision.hosts.end());
        bool hasFunctionsOnThisHost = uniqueHosts.contains(thisHost);

        // Mark the request as being single-host if necessary
        if (conf.noSingleHostOptimisations == 0) {
            std::set<std::string> thisHostUniset = { thisHost };
            isSingleHost = (uniqueHosts == thisHostUniset) && isMaster;
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
    */

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
        // TODO: can we set this in the request?
        snapshotKey = firstMsg.snapshotkey();
    }

    if (isMain && !snapshotKey.empty()) {
        auto snap = reg.getSnapshot(snapshotKey);

        for (const auto& host : orderedHosts) {
            if (host == thisHost) {
                continue;
            }

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
    } else if (!snapshotKey.empty() && isMigration) {
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

    // For threads we only need one executor, for anything else we want
    // one Executor per function in flight.

    SPDLOG_DEBUG(
      "Executing {} calls from {} at host {}", nMessages, funcStr, thisHost);

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
        std::vector<int> thisHostIdxs(req->messages_size());
        std::iota(thisHostIdxs.begin(), thisHostIdxs.end(), 0);
        e->executeTasks(thisHostIdxs, req);
    } else {
        // Non-threads require one executor per task
        for (int i = 0; i < req->messages_size(); i++) {
            faabric::Message& localMsg = req->mutable_messages()->at(i);

            /* TODO: planner-based message-result-promise handling
            if (localMsg.executeslocally()) {
                faabric::util::UniqueLock resultsLock(localResultsMutex);
                localResults.insert(
                  { localMsg.id(), std::make_shared<MessageLocalResult>() });
            }
            */

            std::shared_ptr<Executor> e = claimExecutor(localMsg, lock);
            e->executeTasks({ i }, req);
        }
    }

    /* TODO: remove me
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
    */

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

void Scheduler::broadcastSnapshotDelete(const faabric::Message& msg,
                                        const std::string& snapshotKey)
{
    const std::set<std::string> thisRegisteredHosts =
      getFunctionRegisteredHosts(msg);

    for (auto host : thisRegisteredHosts) {
        if (host == thisHost) {
            continue;
        }
        getSnapshotClient(host)->deleteSnapshot(snapshotKey);
    }
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

std::shared_ptr<faabric::planner::PlannerClient> Scheduler::getPlannerClient()
{
    auto plannerHost = faabric::util::getIPFromHostname(
      faabric::util::getSystemConfig().plannerHost);
    auto client = plannerClient.get(plannerHost).value_or(nullptr);
    if (client == nullptr) {
        SPDLOG_DEBUG("Adding new planner client for {}", plannerHost);
        client = plannerClient.tryEmplaceShared(plannerHost).second;
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

/* TODO: remove me
void Scheduler::broadcastFlush()
{
    faabric::util::FullLock lock(mx);
    // Get all hosts
    std::set<std::string> allHosts = getAvailableHosts();

    // Remove this host from the set
    allHosts.erase(thisHost);

    // Dispatch flush message to all other hosts
    for (const auto& otherHost : allHosts) {
        getFunctionCallClient(otherHost)->sendFlush();
    }

    lock.unlock();
    flushLocally();
}
*/

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
    // Record which host did the execution
    msg.set_executedhost(faabric::util::getSystemConfig().endpointHost);

    // Set finish timestamp
    msg.set_finishtimestamp(faabric::util::getGlobalClock().epochMillis());

    // If someone is already waiting for this result locally, set it so that
    // we avoid going back and forth to the planner
    {
        faabric::util::UniqueLock lock(plannerResultsMutex);

        auto msgPtr = std::make_shared<faabric::Message>(msg);
        if (plannerResults.find(msg.id()) != plannerResults.end()) {
            plannerResults.at(msg.id())->setValue(msgPtr);
        }
    }

    getPlannerClient()->setMessageResult(
      std::make_shared<faabric::Message>(msg));
}

// TODO: this method is used to set the message result sent from the planner
// for the functions that are waiting on it. There's some duplication in names
// between FunctionCall Planner and here in the scheduler
void Scheduler::setMessageResult(std::shared_ptr<faabric::Message> msg)
{
    faabric::util::UniqueLock lock(plannerResultsMutex);

    if (plannerResults.find(msg->id()) == plannerResults.end()) {
        SPDLOG_DEBUG(
          "Ignoring setting message that is already set (id: {}, app: {})",
          msg->id(),
          msg->appid());
        return;
    }

    plannerResults.at(msg->id())->setValue(msg);
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

    // Lastly, publish the result to the planner to release the slot
    // TODO: do we do this here?
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

// This method gets the function result from the planner in a blocking fashion.
// Even though the results are stored in the planner, we want to block in the
// client (i.e. here) not in the planner, to avoid consuming planner threads
// or adding complexity there. This method will first query the planner once
// for the result. If its not there, the planner will register this host's
// interest, and send a function call setting the message result. In the
// meantime, we wait on a promise
// TODO: this method could be optimised, as there's a chance that we are
// setting the message result from this same host, so we would not need to
// go through the planner
faabric::Message Scheduler::getFunctionResult(const faabric::Message& msg,
                                              int timeoutMs)
{
    auto msgPtr = std::make_shared<faabric::Message>(msg);
    auto resMsgPtr = getPlannerClient()->getMessageResult(msgPtr);
    int msgId = msgPtr->id();

    // If when we first check the message it is there, return. Otherwise, we
    // will have told the planner we want the result
    if (resMsgPtr) {
        return *resMsgPtr;
    }

    // If we are here, we need to wait for the planner to let us know that
    // the message is ready. To do so, we need to set a promise at the message
    // id. We do so immediately after returning, so that we don't race with
    // the planner sending the result back
    std::future<std::shared_ptr<faabric::Message>> fut;
    {
        faabric::util::UniqueLock lock(plannerResultsMutex);

        // TODO: what happens if someone else has already called
        // getFunctionResult?
        if (plannerResults.find(msgId) == plannerResults.end()) {
            plannerResults.insert(
              { msgId,
                std::make_shared<faabric::util::MessageResultPromise>() });
        }

        fut = plannerResults.at(msgId)->promise.get_future();
    }

    bool isBlocking = timeoutMs > 0;

    while (true) {
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

        return *fut.get();
    }
}

/*
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
        std::shared_ptr<MessageResultPromise> mlr;
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
            std::shared_ptr<MessageResultPromise> mlr;
            asio::posix::stream_descriptor dsc;
            std::function<void(faabric::Message&)> handler;

            MlrAwaiter(unsigned int messageId,
                       Scheduler* sched,
                       std::shared_ptr<MessageResultPromise> mlr,
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
*/

/* TODO: remove me
faabric::HostResources Scheduler::getThisHostResources()
{
    faabric::util::SharedLock lock(mx);
    faabric::HostResources hostResources = thisHostResources;
    hostResources.set_usedslots(
      this->thisHostUsedSlots.load(std::memory_order_acquire));
    return hostResources;
}
*/

// We keep this method for backwards-compatibility
void Scheduler::setThisHostResources(faabric::HostResources& res)
{
    addHostToGlobalSet(thisHost, std::make_shared<faabric::HostResources>(res));
}

/* TODO: remove me
faabric::HostResources Scheduler::getHostResources(const std::string& host)
{
    SPDLOG_TRACE("Requesting resources from {}", host);
    return getFunctionCallClient(host)->getResources();
}
*/

// --------------------------------------------
// EXECUTION GRAPH
// --------------------------------------------

#define CHAINED_SET_PREFIX "chained_"
std::string getChainedKey(unsigned int msgId)
{
    return std::string(CHAINED_SET_PREFIX) + std::to_string(msgId);
}

// TODO: fix chain functions with the planner
void Scheduler::logChainedFunction(unsigned int parentMessageId,
                                   unsigned int chainedMessageId)
{
    redis::Redis& redis = redis::Redis::getQueue();

    const std::string& key = getChainedKey(parentMessageId);
    redis.sadd(key, std::to_string(chainedMessageId));
    redis.expire(key, STATUS_KEY_EXPIRY);
}

// TODO: fix chain functions with the planner
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

// To check for migration opportunities, we request a scheduling decision for
// the same batch execute request
std::shared_ptr<faabric::PendingMigration>
Scheduler::checkForMigrationOpportunities(faabric::Message& msg,
                                          int overwriteNewGroupId)
{
    int appId = msg.appid();
    int groupId = msg.groupid();
    int groupIdx = msg.groupidx();
    SPDLOG_DEBUG("Message {}:{}:{} checking for migration opportunities",
                 appId,
                 groupId,
                 groupIdx);

    // TODO: maybe we could move this into a broker-specific function?
    int newGroupId = 0;
    if (groupIdx == 0) {
        auto req =
          faabric::util::batchExecFactory(msg.user(), msg.function(), 1);
        req->set_appid(msg.appid());
        req->set_groupid(msg.groupid());
        req->set_type(faabric::BatchExecuteRequest::DIST_CHANGE);
        auto decision = getPlannerClient()->callFunctions(req);
        newGroupId = decision.groupId;

        // Send the new group id to all the members of the group
        auto groupIdxs = broker.getIdxsRegisteredForGroup(groupId);
        groupIdxs.erase(0);
        for (const auto& recvIdx : groupIdxs) {
            broker.sendMessage(
              groupId, 0, recvIdx, BYTES_CONST(&newGroupId), sizeof(int));
        }
    } else if (overwriteNewGroupId == 0) {
        std::vector<uint8_t> bytes = broker.recvMessage(groupId, 0, groupIdx);
        newGroupId = faabric::util::bytesToInt(bytes);
    } else {
        // In some settings, like tests, we already know the new group id, so
        // we can set it here
        newGroupId = overwriteNewGroupId;
    }

    bool appMustMigrate = newGroupId != groupId;
    if (!appMustMigrate) {
        return nullptr;
    }

    msg.set_groupid(newGroupId);
    broker.waitForMappingsOnThisHost(newGroupId);
    std::string newHost = broker.getHostForReceiver(newGroupId, groupIdx);

    auto migration = std::make_shared<faabric::PendingMigration>();
    migration->set_appid(appId);
    migration->set_groupid(newGroupId);
    migration->set_groupidx(groupIdx);
    migration->set_srchost(thisHost);
    migration->set_dsthost(newHost);

    return migration;
}
}
