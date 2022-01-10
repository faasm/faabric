#include <faabric/proto/faabric.pb.h>
#include <faabric/redis/Redis.h>
#include <faabric/scheduler/ExecutorFactory.h>
#include <faabric/scheduler/FunctionCallClient.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/snapshot/SnapshotClient.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/transport/PointToPointBroker.h>
#include <faabric/util/environment.h>
#include <faabric/util/func.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>
#include <faabric/util/memory.h>
#include <faabric/util/message.h>
#include <faabric/util/network.h>
#include <faabric/util/random.h>
#include <faabric/util/scheduling.h>
#include <faabric/util/snapshot.h>
#include <faabric/util/string_tools.h>
#include <faabric/util/testing.h>
#include <faabric/util/timing.h>

#include <unordered_set>

#define FLUSH_TIMEOUT_MS 10000
#define GET_EXEC_GRAPH_SLEEP_MS 500
#define MAX_GET_EXEC_GRAPH_RETRIES 3

using namespace faabric::util;
using namespace faabric::snapshot;

namespace faabric::scheduler {

// 0MQ sockets are not thread-safe, and opening them and closing them from
// different threads messes things up. However, we don't want to constatnly
// create and recreate them to make calls in the scheduler, therefore we cache
// them in TLS, and perform thread-specific tidy-up.
static thread_local std::unordered_map<std::string,
                                       faabric::scheduler::FunctionCallClient>
  functionCallClients;

static thread_local std::unordered_map<std::string,
                                       faabric::snapshot::SnapshotClient>
  snapshotClients;

Scheduler& getScheduler()
{
    static Scheduler sch;
    return sch;
}

Scheduler::Scheduler()
  : thisHost(faabric::util::getSystemConfig().endpointHost)
  , conf(faabric::util::getSystemConfig())
  , broker(faabric::transport::getPointToPointBroker())
{
    // Set up the initial resources
    int cores = faabric::util::getUsableCores();
    thisHostResources.set_slots(cores);
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

    functionCallClients.clear();
    snapshotClients.clear();
}

void Scheduler::reset()
{
    SPDLOG_DEBUG("Resetting scheduler");

    resetThreadLocalCache();

    // Shut down all Executors
    // Executor shutdown takes a lock itself, so "finish" executors without the
    // lock.
    decltype(executors) executorsList;
    decltype(deadExecutors) deadExecutorsList;
    {
        faabric::util::FullLock lock(mx);
        executorsList = executors;
    }
    for (auto& p : executorsList) {
        for (auto& e : p.second) {
            e->finish();
        }
    }
    executorsList.clear();
    {
        faabric::util::FullLock lock(mx);
        deadExecutorsList = deadExecutors;
    }
    for (auto& e : deadExecutors) {
        e->finish();
    }
    deadExecutorsList.clear();

    functionMigrationThread.stop();

    faabric::util::FullLock lock(mx);
    executors.clear();
    deadExecutors.clear();

    // Ensure host is set correctly
    thisHost = faabric::util::getSystemConfig().endpointHost;

    // Reset resources
    thisHostResources = faabric::HostResources();
    thisHostResources.set_slots(faabric::util::getUsableCores());

    // Reset scheduler state
    availableHostsCache.clear();
    registeredHosts.clear();
    threadResults.clear();
    pushedSnapshotsMap.clear();

    // Reset function migration tracking
    inFlightRequests.clear();
    pendingMigrations.clear();

    // Records
    recordedMessagesAll.clear();
    recordedMessagesLocal.clear();
    recordedMessagesShared.clear();
}

void Scheduler::shutdown()
{
    reset();

    removeHostFromGlobalSet(thisHost);
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
    const std::string funcStr = faabric::util::funcToString(msg, false);
    return (int)registeredHosts[funcStr].size();
}

std::set<std::string> Scheduler::getFunctionRegisteredHosts(
  const faabric::Message& msg,
  bool acquireLock)
{
    faabric::util::SharedLock lock;
    if (acquireLock) {
        lock = faabric::util::SharedLock(mx);
    }
    const std::string funcStr = faabric::util::funcToString(msg, false);
    return registeredHosts[funcStr];
}

void Scheduler::removeRegisteredHost(const std::string& host,
                                     const faabric::Message& msg)
{
    faabric::util::FullLock lock(mx);
    const std::string funcStr = faabric::util::funcToString(msg, false);
    registeredHosts[funcStr].erase(host);
}

void Scheduler::vacateSlot()
{
    thisHostUsedSlots.fetch_sub(1, std::memory_order_acq_rel);
}

void Scheduler::notifyExecutorShutdown(Executor* exec,
                                       const faabric::Message& msg)
{
    faabric::util::FullLock lock(mx);

    SPDLOG_TRACE("Shutting down executor {}", exec->id);

    std::string funcStr = faabric::util::funcToString(msg, false);

    // Find in list of executors
    int execIdx = -1;
    std::vector<std::shared_ptr<Executor>>& thisExecutors = executors[funcStr];
    for (int i = 0; i < thisExecutors.size(); i++) {
        if (thisExecutors.at(i).get() == exec) {
            execIdx = i;
            break;
        }
    }

    // We assume it's been found or something has gone very wrong
    assert(execIdx >= 0);

    // Record as dead, remove from live executors
    // Note that this is necessary as this method may be called from a worker
    // thread, so we can't fully clean up the executor without having a deadlock
    deadExecutors.emplace_back(thisExecutors.at(execIdx));
    thisExecutors.erase(thisExecutors.begin() + execIdx);

    if (thisExecutors.empty()) {
        SPDLOG_TRACE("No remaining executors for {}", funcStr);

        // Unregister if this was the last executor for that function
        bool isMaster = thisHost == msg.masterhost();
        if (!isMaster) {
            faabric::UnregisterRequest req;
            req.set_host(thisHost);
            *req.mutable_function() = msg;

            getFunctionCallClient(msg.masterhost()).unregister(req);
        }
    }
}

faabric::util::SchedulingDecision Scheduler::callFunctions(
  std::shared_ptr<faabric::BatchExecuteRequest> req,
  faabric::util::SchedulingTopologyHint topologyHint)
{
    // Note, we assume all the messages are for the same function and have the
    // same master host
    faabric::Message& firstMsg = req->mutable_messages()->at(0);
    std::string masterHost = firstMsg.masterhost();
    if (masterHost.empty()) {
        std::string funcStrWithId = faabric::util::funcToString(firstMsg, true);
        SPDLOG_ERROR("Request {} has no master host", funcStrWithId);
        throw std::runtime_error("Message with no master host");
    }

    // If we're not the master host, we need to forward the request back to the
    // master host. This will only happen if a nested batch execution happens.
    if (topologyHint != faabric::util::SchedulingTopologyHint::FORCE_LOCAL &&
        masterHost != thisHost) {
        std::string funcStr = faabric::util::funcToString(firstMsg, false);
        SPDLOG_DEBUG("Forwarding {} back to master {}", funcStr, masterHost);

        getFunctionCallClient(masterHost).executeFunctions(req);
        SchedulingDecision decision(firstMsg.appid(), firstMsg.groupid());
        decision.returnHost = masterHost;
        return decision;
    }

    faabric::util::FullLock lock(mx);

    SchedulingDecision decision = makeSchedulingDecision(req, topologyHint);

    // Send out point-to-point mappings if necessary (unless being forced to
    // execute locally, in which case they will be transmitted from the
    // master)
    if (topologyHint != faabric::util::SchedulingTopologyHint::FORCE_LOCAL &&
        (firstMsg.groupid() > 0)) {
        broker.setAndSendMappingsFromSchedulingDecision(decision);
    }

    // Pass decision as hint
    return doCallFunctions(req, decision, lock);
}

faabric::util::SchedulingDecision Scheduler::makeSchedulingDecision(
  std::shared_ptr<faabric::BatchExecuteRequest> req,
  faabric::util::SchedulingTopologyHint topologyHint)
{
    int nMessages = req->messages_size();
    faabric::Message& firstMsg = req->mutable_messages()->at(0);
    std::string funcStr = faabric::util::funcToString(firstMsg, false);

    // If topology hints are disabled, unset the provided topology hint
    if (conf.noTopologyHints == "on" &&
        topologyHint != faabric::util::SchedulingTopologyHint::NORMAL) {
        SPDLOG_WARN("Ignoring topology hint passed to scheduler as hints are "
                    "disabled in the config");
        topologyHint = faabric::util::SchedulingTopologyHint::NORMAL;
    }

    std::vector<std::string> hosts;
    if (topologyHint == faabric::util::SchedulingTopologyHint::FORCE_LOCAL) {
        // We're forced to execute locally here so we do all the messages
        for (int i = 0; i < nMessages; i++) {
            hosts.push_back(thisHost);
        }
    } else {
        // At this point we know we're the master host, and we've not been
        // asked to force full local execution.

        // Work out how many we can handle locally
        int slots = thisHostResources.slots();

        // Work out available cores, flooring at zero
        int available =
          slots - this->thisHostUsedSlots.load(std::memory_order_acquire);
        available = std::max<int>(available, 0);

        // Claim as many as we can
        int nLocally = std::min<int>(available, nMessages);

        // Add those that can be executed locally
        for (int i = 0; i < nLocally; i++) {
            hosts.push_back(thisHost);
        }

        // If some are left, we need to distribute.
        // First try and do so on already registered hosts.
        int remainder = nMessages - nLocally;
        if (remainder > 0) {
            std::set<std::string>& thisRegisteredHosts =
              registeredHosts[funcStr];

            for (const auto& h : thisRegisteredHosts) {
                // Work out resources on this host
                faabric::HostResources r = getHostResources(h);
                int available = r.slots() - r.usedslots();
                int nOnThisHost = std::min(available, remainder);

                // Under the NEVER_ALONE topology hint, we never choose a host
                // unless we can schedule at least two requests in it.
                if (topologyHint ==
                      faabric::util::SchedulingTopologyHint::NEVER_ALONE &&
                    nOnThisHost < 2) {
                    continue;
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

        // Now schedule to unregistered hosts if there are messages left
        if (remainder > 0) {
            std::vector<std::string> unregisteredHosts =
              getUnregisteredHosts(funcStr);

            for (const auto& h : unregisteredHosts) {
                // Skip if this host
                if (h == thisHost) {
                    continue;
                }

                // Work out resources on this host
                faabric::HostResources r = getHostResources(h);
                int available = r.slots() - r.usedslots();
                int nOnThisHost = std::min(available, remainder);

                if (topologyHint ==
                      faabric::util::SchedulingTopologyHint::NEVER_ALONE &&
                    nOnThisHost < 2) {
                    continue;
                }

                // Register the host if it's exected a function
                if (nOnThisHost > 0) {
                    registeredHosts[funcStr].insert(h);
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
    assert(hosts.size() == nMessages);

    // Set up decision
    SchedulingDecision decision(firstMsg.appid(), firstMsg.groupid());
    for (int i = 0; i < hosts.size(); i++) {
        decision.addMessage(hosts.at(i), req->messages().at(i));
    }

    return decision;
}

faabric::util::SchedulingDecision Scheduler::callFunctions(
  std::shared_ptr<faabric::BatchExecuteRequest> req,
  faabric::util::SchedulingDecision& hint)
{
    faabric::util::FullLock lock(mx);
    return doCallFunctions(req, hint, lock);
}

faabric::util::SchedulingDecision Scheduler::doCallFunctions(
  std::shared_ptr<faabric::BatchExecuteRequest> req,
  faabric::util::SchedulingDecision& decision,
  faabric::util::FullLock& lock)
{
    faabric::Message& firstMsg = req->mutable_messages()->at(0);
    std::string funcStr = faabric::util::funcToString(firstMsg, false);
    int nMessages = req->messages_size();

    if (decision.hosts.size() != nMessages) {
        SPDLOG_ERROR(
          "Passed decision for {} with {} messages, but request has {}",
          funcStr,
          decision.hosts.size(),
          nMessages);
        throw std::runtime_error("Invalid scheduler hint for messages");
    }

    // Record in-flight request if function desires to be migrated
    // NOTE: ideally, instead of allowing the applications to specify a check
    // period, we would have a default one (overwritable through an env.
    // variable), and apps would just opt in/out of being migrated. We set
    // the actual check period instead to ease with experiments.
    if (firstMsg.migrationcheckperiod() > 0) {
        bool startMigrationThread = inFlightRequests.size() == 0;

        if (inFlightRequests.find(decision.appId) != inFlightRequests.end()) {
            // MPI applications are made up of two different requests: the
            // original one (with one message) and the second one (with
            // world size - 1 messages) created during world creation time.
            // Thus, to correctly track migration opportunities we must merge
            // both. We append the batch request to the original one (instead
            // of the other way around) not to affect the rest of this methods
            // functionality.
            if (firstMsg.ismpi()) {
                startMigrationThread = false;
                auto originalReq = inFlightRequests[decision.appId].first;
                auto originalDecision = inFlightRequests[decision.appId].second;
                assert(req->messages_size() == firstMsg.mpiworldsize() - 1);
                for (int i = 0; i < firstMsg.mpiworldsize() - 1; i++) {
                    // Append message to original request
                    auto* newMsgPtr = originalReq->add_messages();
                    faabric::util::copyMessage(&req->messages().at(i),
                                               newMsgPtr);
                    // Append message to original decision
                    originalDecision->addMessage(decision.hosts.at(i),
                                                 req->messages().at(i));
                }
            } else {
                SPDLOG_ERROR("There is already an in-flight request for app {}",
                             firstMsg.appid());
                throw std::runtime_error("App already in-flight");
            }
        } else {
            auto decisionPtr =
              std::make_shared<faabric::util::SchedulingDecision>(decision);
            inFlightRequests[decision.appId] = std::make_pair(req, decisionPtr);
        }

        // Decide wether we have to start the migration thread or not
        if (startMigrationThread) {
            functionMigrationThread.start(firstMsg.migrationcheckperiod());
        } else if (firstMsg.migrationcheckperiod() !=
                   functionMigrationThread.wakeUpPeriodSeconds) {
            SPDLOG_WARN("Ignoring migration check period for app {} as the"
                        "migration thread is already running with a different"
                        " check period (provided: {}, current: {})",
                        firstMsg.appid(),
                        firstMsg.migrationcheckperiod(),
                        functionMigrationThread.wakeUpPeriodSeconds);
        }
    }

    // NOTE: we want to schedule things on this host _last_, otherwise functions
    // may start executing before all messages have been dispatched, thus
    // slowing the remaining scheduling.
    // Therefore we want to create a list of unique hosts, with this host last.
    std::vector<std::string> orderedHosts;
    {
        std::set<std::string> uniqueHosts(decision.hosts.begin(),
                                          decision.hosts.end());
        bool hasFunctionsOnThisHost = uniqueHosts.contains(thisHost);

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

        snapshotKey = faabric::util::getMainThreadSnapshotKey(firstMsg);
    } else {
        snapshotKey = firstMsg.snapshotkey();
    }

    if (!snapshotKey.empty()) {
        auto snap =
          faabric::snapshot::getSnapshotRegistry().getSnapshot(snapshotKey);

        for (const auto& host : getFunctionRegisteredHosts(firstMsg, false)) {
            SnapshotClient& c = getSnapshotClient(host);

            // See if we've already pushed this snapshot to the given host,
            // if so, just push the diffs that have occurred in this main thread
            if (pushedSnapshotsMap[snapshotKey].contains(host)) {
                std::vector<faabric::util::SnapshotDiff> snapshotDiffs =
                  snap->getTrackedChanges();

                c.pushSnapshotUpdate(snapshotKey, snap, snapshotDiffs);
            } else {
                c.pushSnapshot(snapshotKey, snap);
                pushedSnapshotsMap[snapshotKey].insert(host);
            }
        }

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
                            std::promise<
                              std::unique_ptr<faabric::Message>>() });
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
            getFunctionCallClient(host).executeFunctions(hostRequest);
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
  const std::string& funcStr,
  bool noCache)
{
    // Load the list of available hosts
    if (availableHostsCache.empty() || noCache) {
        availableHostsCache = getAvailableHosts();
    }

    // At this point we know we need to enlist unregistered hosts
    std::set<std::string>& thisRegisteredHosts = registeredHosts[funcStr];

    std::vector<std::string> unregisteredHosts;

    std::set_difference(
      availableHostsCache.begin(),
      availableHostsCache.end(),
      thisRegisteredHosts.begin(),
      thisRegisteredHosts.end(),
      std::inserter(unregisteredHosts, unregisteredHosts.begin()));

    // If we've not got any, try again without caching
    if (unregisteredHosts.empty() && !noCache) {
        return getUnregisteredHosts(funcStr, true);
    }

    return unregisteredHosts;
}

void Scheduler::broadcastSnapshotDelete(const faabric::Message& msg,
                                        const std::string& snapshotKey)
{
    std::string funcStr = faabric::util::funcToString(msg, false);
    std::set<std::string>& thisRegisteredHosts = registeredHosts[funcStr];

    for (auto host : thisRegisteredHosts) {
        SnapshotClient& c = getSnapshotClient(host);
        c.deleteSnapshot(snapshotKey);
    }
}

void Scheduler::callFunction(faabric::Message& msg, bool forceLocal)
{
    // TODO - avoid this copy
    auto req = faabric::util::batchExecFactory();
    *req->add_messages() = msg;

    // Specify that this is a normal function, not a thread
    req->set_type(req->FUNCTIONS);

    // Make the call
    if (forceLocal) {
        callFunctions(req, faabric::util::SchedulingTopologyHint::FORCE_LOCAL);
    } else {
        callFunctions(req);
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

FunctionCallClient& Scheduler::getFunctionCallClient(
  const std::string& otherHost)
{
    if (functionCallClients.find(otherHost) == functionCallClients.end()) {
        SPDLOG_DEBUG("Adding new function call client for {}", otherHost);
        functionCallClients.emplace(otherHost, otherHost);
    }

    return functionCallClients.at(otherHost);
}

SnapshotClient& Scheduler::getSnapshotClient(const std::string& otherHost)
{
    if (snapshotClients.find(otherHost) == snapshotClients.end()) {
        SPDLOG_DEBUG("Adding new snapshot client for {}", otherHost);
        snapshotClients.emplace(otherHost, otherHost);
    }

    return snapshotClients.at(otherHost);
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
        getFunctionCallClient(otherHost).sendFlush();
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

    // Record which host did the execution
    msg.set_executedhost(faabric::util::getSystemConfig().endpointHost);

    // Set finish timestamp
    msg.set_finishtimestamp(faabric::util::getGlobalClock().epochMillis());

    if (msg.executeslocally()) {
        faabric::util::UniqueLock resultsLock(localResultsMutex);

        auto it = localResults.find(msg.id());
        if (it != localResults.end()) {
            it->second.set_value(std::make_unique<faabric::Message>(msg));
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

    // Remove the app from in-flight map if still there, and this host is the
    // master host for the message
    if (msg.masterhost() == thisHost) {
        removePendingMigration(msg.appid());
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

void Scheduler::setThreadResult(const faabric::Message& msg,
                                int32_t returnValue)
{
    bool isMaster = msg.masterhost() == conf.endpointHost;

    if (isMaster) {
        setThreadResultLocally(msg.id(), returnValue);
    } else {
        SnapshotClient& c = getSnapshotClient(msg.masterhost());
        c.pushThreadResult(msg.id(), returnValue);
    }
}

void Scheduler::pushSnapshotDiffs(
  const faabric::Message& msg,
  const std::string& snapshotKey,
  const std::vector<faabric::util::SnapshotDiff>& diffs)
{
    if (diffs.empty()) {
        return;
    }

    bool isMaster = msg.masterhost() == conf.endpointHost;

    if (isMaster) {
        SPDLOG_ERROR("{} pushing snapshot diffs for {} on master",
                     faabric::util::funcToString(msg, false),
                     snapshotKey);
        throw std::runtime_error("Cannot push snapshot diffs on master");
    }

    SnapshotClient& c = getSnapshotClient(msg.masterhost());
    c.pushSnapshotDiffs(snapshotKey, diffs);
}

void Scheduler::setThreadResultLocally(uint32_t msgId, int32_t returnValue)
{
    faabric::util::FullLock lock(mx);
    SPDLOG_DEBUG("Setting result for thread {} to {}", msgId, returnValue);
    threadResults.at(msgId).set_value(returnValue);
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
            fut = it->second.get_future();
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

faabric::HostResources Scheduler::getThisHostResources()
{
    faabric::util::SharedLock lock(mx);
    faabric::HostResources hostResources = thisHostResources;
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
    return getFunctionCallClient(host).getResources();
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

void Scheduler::checkForMigrationOpportunities()
{
    std::vector<std::shared_ptr<faabric::PendingMigrations>>
      tmpPendingMigrations;

    {
        // Acquire a shared lock to read from the in-flight requests map
        faabric::util::SharedLock lock(mx);

        tmpPendingMigrations = doCheckForMigrationOpportunities();
    }

    // If we find migration opportunites
    if (tmpPendingMigrations.size() > 0) {
        // Acquire full lock to write to the pending migrations map
        faabric::util::FullLock lock(mx);

        for (auto msgPtr : tmpPendingMigrations) {
            // First, broadcast the pending migrations to other hosts
            broadcastAddPendingMigration(msgPtr);
            // Second, update our local records
            pendingMigrations[msgPtr->appid()] = std::move(msgPtr);
        }
    }
}

void Scheduler::broadcastAddPendingMigration(
  std::shared_ptr<faabric::PendingMigrations> pendingMigrations)
{
    // Get all hosts for the to-be migrated app
    auto msg = pendingMigrations->migrations().at(0).msg();
    std::string funcStr = faabric::util::funcToString(msg, false);
    std::set<std::string>& thisRegisteredHosts = registeredHosts[funcStr];

    // Remove this host from the set
    registeredHosts.erase(thisHost);

    // Send pending migrations to all involved hosts
    for (auto& otherHost : thisRegisteredHosts) {
        getFunctionCallClient(otherHost).sendAddPendingMigration(
          pendingMigrations);
    }
}

std::shared_ptr<faabric::PendingMigrations> Scheduler::canAppBeMigrated(
  uint32_t appId)
{
    faabric::util::SharedLock lock(mx);

    if (pendingMigrations.find(appId) == pendingMigrations.end()) {
        return nullptr;
    }

    return pendingMigrations[appId];
}

void Scheduler::addPendingMigration(
  std::shared_ptr<faabric::PendingMigrations> pMigration)
{
    faabric::util::FullLock lock(mx);

    auto msg = pMigration->migrations().at(0).msg();
    if (pendingMigrations.find(msg.appid()) != pendingMigrations.end()) {
        SPDLOG_ERROR("Received remote request to add a pending migration for "
                     "app {}, but already recorded another migration request"
                     " for the same app.",
                     msg.appid());
        throw std::runtime_error("Remote request for app already there");
    }

    pendingMigrations[msg.appid()] = pMigration;
}

void Scheduler::removePendingMigration(uint32_t appId)
{
    faabric::util::FullLock lock(mx);

    inFlightRequests.erase(appId);
    pendingMigrations.erase(appId);
}

std::vector<std::shared_ptr<faabric::PendingMigrations>>
Scheduler::doCheckForMigrationOpportunities(
  faabric::util::MigrationStrategy migrationStrategy)
{
    std::vector<std::shared_ptr<faabric::PendingMigrations>>
      pendingMigrationsVec;

    // For each in-flight request that has opted in to be migrated,
    // check if there is an opportunity to migrate
    for (const auto& app : inFlightRequests) {
        auto req = app.second.first;
        auto originalDecision = *app.second.second;

        // If we have already recorded a pending migration for this req,
        // skip
        if (canAppBeMigrated(originalDecision.appId) != nullptr) {
            SPDLOG_TRACE("Skipping app {} as migration opportunity has "
                         "already been recorded",
                         originalDecision.appId);
            continue;
        }

        faabric::PendingMigrations msg;
        msg.set_appid(originalDecision.appId);

        if (migrationStrategy == faabric::util::MigrationStrategy::BIN_PACK) {
            // We assume the batch was originally scheduled using
            // bin-packing, thus the scheduling decision has at the begining
            // (left) the hosts with the most allocated requests, and at the
            // end (right) the hosts with the fewest. To check for migration
            // oportunities, we compare a pointer to the possible
            // destination of the migration (left), with one to the possible
            // source of the migration (right). NOTE - this is a slight
            // simplification, but makes the code simpler.
            auto left = originalDecision.hosts.begin();
            auto right = originalDecision.hosts.end() - 1;
            faabric::HostResources r = (*left == thisHost)
                                         ? getThisHostResources()
                                         : getHostResources(*left);
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
                if (*left == *right) {
                    --right;
                    continue;
                }

                // If the left pointer (possible destination of the
                // migration) is out of available resources, no migration
                // opportunity, and must check another possible destination
                // of migration
                if (nAvailable() == 0) {
                    auto oldHost = *left;
                    ++left;
                    if (*left != oldHost) {
                        r = (*left == thisHost) ? getThisHostResources()
                                                : getHostResources(*left);
                    }
                    continue;
                }

                // If each pointer points to a request scheduled in a
                // different host, and the possible destination has slots,
                // there is a migration opportunity
                auto* migration = msg.add_migrations();
                migration->set_srchost(*right);
                migration->set_dsthost(*left);

                faabric::Message* msgPtr =
                  &(*(req->mutable_messages()->begin() +
                      std::distance(originalDecision.hosts.begin(), right)));
                auto* migrationMsgPtr = migration->mutable_msg();
                faabric::util::copyMessage(msgPtr, migrationMsgPtr);
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
            pendingMigrationsVec.emplace_back(
              std::make_shared<faabric::PendingMigrations>(msg));
            SPDLOG_DEBUG("Detected migration opportunity for app: {}",
                         msg.appid());
        } else {
            SPDLOG_DEBUG("No migration opportunity detected for app: {}",
                         msg.appid());
        }
    }

    return pendingMigrationsVec;
}
}
