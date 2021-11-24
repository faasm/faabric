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
#include <faabric/util/random.h>
#include <faabric/util/scheduling.h>
#include <faabric/util/snapshot.h>
#include <faabric/util/string_tools.h>
#include <faabric/util/testing.h>
#include <faabric/util/timing.h>

#include <unordered_set>

#define FLUSH_TIMEOUT_MS 10000

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
    auto tid = (pid_t)syscall(SYS_gettid);
    SPDLOG_DEBUG("Resetting scheduler thread-local cache for thread {}", tid);

    functionCallClients.clear();
    snapshotClients.clear();
}

void Scheduler::reset()
{
    SPDLOG_DEBUG("Resetting scheduler");

    resetThreadLocalCache();

    // Shut down all Executors
    for (auto& p : executors) {
        for (auto& e : p.second) {
            e->finish();
        }
    }

    for (auto& e : deadExecutors) {
        e->finish();
    }

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
    const std::string funcStr = faabric::util::funcToString(msg, false);
    return executors[funcStr].size();
}

int Scheduler::getFunctionRegisteredHostCount(const faabric::Message& msg)
{
    const std::string funcStr = faabric::util::funcToString(msg, false);
    return (int)registeredHosts[funcStr].size();
}

std::set<std::string> Scheduler::getFunctionRegisteredHosts(
  const faabric::Message& msg)
{
    const std::string funcStr = faabric::util::funcToString(msg, false);
    return registeredHosts[funcStr];
}

void Scheduler::removeRegisteredHost(const std::string& host,
                                     const faabric::Message& msg)
{
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
  bool forceLocal,
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
    if (!forceLocal && masterHost != thisHost) {
        std::string funcStr = faabric::util::funcToString(firstMsg, false);
        SPDLOG_DEBUG("Forwarding {} back to master {}", funcStr, masterHost);

        getFunctionCallClient(masterHost).executeFunctions(req);
        SchedulingDecision decision(firstMsg.appid(), firstMsg.groupid());
        decision.returnHost = masterHost;
        return decision;
    }

    faabric::util::FullLock lock(mx);

    SchedulingDecision decision =
      makeSchedulingDecision(req, forceLocal, topologyHint);

    // Send out point-to-point mappings if necessary (unless being forced to
    // execute locally, in which case they will be transmitted from the
    // master)
    if (!forceLocal && (firstMsg.groupid() > 0)) {
        broker.setAndSendMappingsFromSchedulingDecision(decision);
    }

    // Pass decision as hint
    return doCallFunctions(req, decision, lock);
}

faabric::util::SchedulingDecision Scheduler::makeSchedulingDecision(
  std::shared_ptr<faabric::BatchExecuteRequest> req,
  bool forceLocal,
  faabric::util::SchedulingTopologyHint topologyHint)
{
    int nMessages = req->messages_size();
    faabric::Message& firstMsg = req->mutable_messages()->at(0);
    std::string funcStr = faabric::util::funcToString(firstMsg, false);

    std::vector<std::string> hosts;
    if (forceLocal) {
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

                // Under the pairs topology hint, we never allocate a single
                // non-master request (id != 0) to a host without other
                // requests of the batch
                bool stickToPreviousHost =
                  (topologyHint ==
                     faabric::util::SchedulingTopologyHint::NEVER_ALONE &&
                   nOnThisHost == 1 && hosts.size() > 0);

                if (stickToPreviousHost) {
                    hosts.push_back(hosts.back());
                } else {
                    for (int i = 0; i < nOnThisHost; i++) {
                        hosts.push_back(h);
                    }
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

                bool stickToPreviousHost =
                  (topologyHint ==
                     faabric::util::SchedulingTopologyHint::NEVER_ALONE &&
                   nOnThisHost == 1 && hosts.size() > 0);

                if (stickToPreviousHost) {
                    hosts.push_back(hosts.back());
                } else {
                    // Register the host if it's exected a function
                    if (nOnThisHost > 0) {
                        registeredHosts[funcStr].insert(h);
                    }

                    for (int i = 0; i < nOnThisHost; i++) {
                        hosts.push_back(h);
                    }
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
            SPDLOG_DEBUG(
              "Overloading {}/{} {} locally", remainder, nMessages, funcStr);

            for (int i = 0; i < remainder; i++) {
                hosts.push_back(thisHost);
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
    std::string snapshotKey = firstMsg.snapshotkey();
    if (!snapshotKey.empty()) {
        for (const auto& host : getFunctionRegisteredHosts(firstMsg)) {
            SnapshotClient& c = getSnapshotClient(host);
            faabric::util::SnapshotData snapshotData =
              faabric::snapshot::getSnapshotRegistry().getSnapshot(snapshotKey);

            // See if we've already pushed this snapshot to the given host,
            // if so, just push the diffs
            if (pushedSnapshotsMap[snapshotKey].contains(host)) {
                std::vector<faabric::util::SnapshotDiff> snapshotDiffs =
                  snapshotData.getDirtyPages();
                c.pushSnapshotDiffs(
                  snapshotKey, firstMsg.groupid(), snapshotDiffs);
            } else {
                c.pushSnapshot(snapshotKey, firstMsg.groupid(), snapshotData);
                pushedSnapshotsMap[snapshotKey].insert(host);
            }
        }
    }

    // Now reset the dirty page tracking just before we start executing
    SPDLOG_DEBUG("Resetting dirty tracking after pushing diffs {}", funcStr);
    faabric::util::resetDirtyTracking();

    // -------------------------------------------
    // EXECTUION
    // -------------------------------------------

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
            faabric::Message msg = req->messages().at(i);

            // Log results if in test mode
            recordedMessagesAll.push_back(msg);
            if (executedHost.empty() || executedHost == thisHost) {
                recordedMessagesLocal.push_back(msg);
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
    callFunctions(req, forceLocal);
}

void Scheduler::clearRecordedMessages()
{
    recordedMessagesAll.clear();
    recordedMessagesLocal.clear();
    recordedMessagesShared.clear();
}

std::vector<faabric::Message> Scheduler::getRecordedMessagesAll()
{
    return recordedMessagesAll;
}

std::vector<faabric::Message> Scheduler::getRecordedMessagesLocal()
{
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
        int nExecutors = thisExecutors.size();
        SPDLOG_DEBUG(
          "Scaling {} from {} -> {}", funcStr, nExecutors, nExecutors + 1);
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
    return thisHost;
}

void Scheduler::broadcastFlush()
{
    // Get all hosts
    std::set<std::string> allHosts = getAvailableHosts();

    // Remove this host from the set
    allHosts.erase(thisHost);

    // Dispatch flush message to all other hosts
    for (auto& otherHost : allHosts) {
        getFunctionCallClient(otherHost).sendFlush();
    }

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
  const std::vector<faabric::util::SnapshotDiff>& diffs)
{
    bool isMaster = msg.masterhost() == conf.endpointHost;

    if (!isMaster && !diffs.empty()) {
        SnapshotClient& c = getSnapshotClient(msg.masterhost());
        c.pushSnapshotDiffs(msg.snapshotkey(), msg.groupid(), diffs);
    }
}

void Scheduler::setThreadResultLocally(uint32_t msgId, int32_t returnValue)
{
    SPDLOG_DEBUG("Setting result for thread {} to {}", msgId, returnValue);
    threadResults[msgId].set_value(returnValue);
}

int32_t Scheduler::awaitThreadResult(uint32_t messageId)
{
    if (threadResults.count(messageId) == 0) {
        SPDLOG_ERROR("Thread {} not registered on this host", messageId);
        throw std::runtime_error("Awaiting unregistered thread");
    }

    return threadResults[messageId].get_future().get();
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
    thisHostResources.set_usedslots(
      this->thisHostUsedSlots.load(std::memory_order_acquire));
    return thisHostResources;
}

void Scheduler::setThisHostResources(faabric::HostResources& res)
{
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
    std::vector<uint8_t> messageBytes = redis.get(statusKey);
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
}
