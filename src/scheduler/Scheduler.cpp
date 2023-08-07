// TODO: re-visit includes after re-factor
#include <faabric/batch-scheduler/DecisionCache.h>
#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/planner/PlannerClient.h>
#include <faabric/planner/planner.pb.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/redis/Redis.h>
#include <faabric/scheduler/ExecutorContext.h>
#include <faabric/scheduler/ExecutorFactory.h>
#include <faabric/scheduler/FunctionCallClient.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/snapshot/SnapshotClient.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/transport/PointToPointBroker.h>
#include <faabric/util/batch.h>
#include <faabric/util/environment.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>
#include <faabric/util/memory.h>
#include <faabric/util/network.h>
#include <faabric/util/random.h>
#include <faabric/util/snapshot.h>
#include <faabric/util/string_tools.h>
#include <faabric/util/testing.h>
#include <faabric/util/timing.h>

#include <sys/eventfd.h>
#include <sys/file.h>
#include <sys/syscall.h>

#include <unordered_set>

#define FLUSH_TIMEOUT_MS 10000

using namespace faabric::util;
using namespace faabric::snapshot;

namespace faabric::scheduler {

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
    auto availableHosts =
      faabric::planner::getPlannerClient().getAvailableHosts();
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
    // Build register host request. Setting the overwrite flag means that we
    // will overwrite whatever records the planner has on this host. We only
    // set it when calling this method for a different host (e.g. in the tests)
    // or when passing an overwrited host-resources (e.g. when calling
    // setThisHostResources)
    auto req = std::make_shared<faabric::planner::RegisterHostRequest>();
    req->mutable_host()->set_ip(hostIp);
    req->set_overwrite(false);
    if (overwriteResources != nullptr) {
        req->mutable_host()->set_slots(overwriteResources->slots());
        req->mutable_host()->set_usedslots(overwriteResources->usedslots());
        req->set_overwrite(true);
    } else if (hostIp == thisHost) {
        req->mutable_host()->set_slots(faabric::util::getUsableCores());
        req->mutable_host()->set_usedslots(0);
    }

    int plannerTimeout = faabric::planner::getPlannerClient().registerHost(req);

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

    faabric::planner::getPlannerClient().removeHost(req);

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

    // Clear the clients
    clearFunctionCallClients();
    clearSnapshotClients();
    faabric::planner::getPlannerClient().clearCache();

    faabric::util::FullLock lock(mx);

    // Ensure host is set correctly
    thisHost = faabric::util::getSystemConfig().endpointHost;

    // Reset resources
    thisHostResources = faabric::HostResources();
    thisHostResources.set_slots(faabric::util::getUsableCores());
    thisHostResources.set_usedslots(0);
    thisHostUsedSlots.store(0, std::memory_order_release);

    // Reset scheduler state
    availableHostsCache.clear();
    registeredHosts.clear();
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
        std::string mainHost = firstMsg.mainhost();

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
        // it's not the main
        if (execs.empty()) {
            SPDLOG_TRACE("No remaining executors for {}", key);

            bool isMaster = thisHost == mainHost;
            if (!isMaster) {
                faabric::UnregisterRequest req;
                req.set_host(thisHost);
                req.set_user(user);
                req.set_function(function);
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

void Scheduler::vacateSlot()
{
    thisHostUsedSlots.fetch_sub(1, std::memory_order_acq_rel);
}

void Scheduler::executeBatch(std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    // -------------------------------------------
    // THREADS TODO FIXME
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
        /* TODO (threads): snapshots
        if (!firstMsg.snapshotkey().empty()) {
            SPDLOG_ERROR("{} should not provide snapshot key for {} threads",
                         funcStr,
                         req->messages().size());

            std::runtime_error("Should not provide snapshot key for threads");
        }

        if (!isSingleHost) {
            snapshotKey = faabric::util::getMainThreadSnapshotKey(firstMsg);
        }
        */
    } else {
        // TODO: can we get the snapshot key from req?
        snapshotKey = req->messages(0).snapshotkey();
    }

    if (!snapshotKey.empty()) {
        auto snap = reg.getSnapshot(snapshotKey);

        for (const auto& host :
             getFunctionRegisteredHosts(req->user(), req->function(), false)) {
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
        /* TODO: fix this elseif
    } else if (!snapshotKey.empty() && isMigration && isForceLocal) {
        // If we are executing a migrated function, we don't need to distribute
        // the snapshot to other hosts, as this snapshot is specific to the
        // to-be-restored function
        auto snap = reg.getSnapshot(snapshotKey);

        // Now reset the tracking on the snapshot before we start executing
        snap->clearTrackedChanges();
        */
    }

    // -------------------------------------------
    // EXECUTION
    // -------------------------------------------
    int nMessages = req->messages_size();

    // Records for tests - copy messages before execution to avoid racing on msg
    size_t recordedMessagesOffset = recordedMessagesAll.size();
    if (faabric::util::isTestMode()) {
        for (int i = 0; i < nMessages; i++) {
            recordedMessagesAll.emplace_back(req->messages().at(i));
        }
    }

    auto funcStr = faabric::util::funcToString(req);

    // -------------------------------------------
    // LOCAL EXECTUION
    // -------------------------------------------
    // For threads we only need one executor, for anything else we want
    // one Executor per function in flight.

    // TODO: can we do this without a lock, or can we put the lock
    // elsewhere?
    faabric::util::FullLock lock(mx);
    if (isThreads) {
        // Threads use the existing executor. We assume there's only
        // one running at a time.
        std::vector<std::shared_ptr<Executor>>& thisExecutors =
          executors[funcStr];

        std::shared_ptr<Executor> e = nullptr;
        if (thisExecutors.empty()) {
            // Create executor if not exists
            e = claimExecutor(*req->mutable_messages(0), lock);
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
        // TODO: make the default of executeTasks take all messages in req?
        std::vector<int> thisHostIdxs(req->messages_size());
        std::iota(thisHostIdxs.begin(), thisHostIdxs.end(), 0);
        e->executeTasks(thisHostIdxs, req);
    } else {
        // Non-threads require one executor per task
        for (int i = 0; i < nMessages; i++) {
            faabric::Message& localMsg = req->mutable_messages()->at(i);

            std::shared_ptr<Executor> e = claimExecutor(localMsg, lock);
            e->executeTasks({ i }, req);
        }
    }

    // Records for tests
    if (faabric::util::isTestMode()) {
        for (int i = 0; i < nMessages; i++) {
            std::string executedHost = thisHost;
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

void Scheduler::flushLocally()
{
    SPDLOG_INFO("Flushing host {}",
                faabric::util::getSystemConfig().endpointHost);

    // Reset this scheduler
    reset();

    // Flush the host
    getExecutorFactory()->flushHost();
}

// TODO(scheduler-cleanup): move this to the planner completely
void Scheduler::setFunctionResult(faabric::Message& msg)
{
    // Record which host did the execution
    msg.set_executedhost(faabric::util::getSystemConfig().endpointHost);

    // Set finish timestamp
    msg.set_finishtimestamp(faabric::util::getGlobalClock().epochMillis());

    // Let the planner know this function has finished execution. This will
    // wake any thread waiting on this result
    faabric::planner::getPlannerClient().setMessageResult(
      std::make_shared<faabric::Message>(msg));
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
    bool isMaster = msg.mainhost() == conf.endpointHost;
    if (isMaster) {
        if (!diffs.empty()) {
            // On main we queue the diffs locally directly, on a remote
            // host we push them back to main
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
        getSnapshotClient(msg.mainhost())
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
    // Update the planner (no lock required)
    addHostToGlobalSet(thisHost, std::make_shared<faabric::HostResources>(res));

    faabric::util::FullLock lock(mx);
    thisHostResources = res;
    this->thisHostUsedSlots.store(res.usedslots(), std::memory_order_release);
}

// --------------------------------------------
// EXECUTION GRAPH
// --------------------------------------------

#define CHAINED_SET_PREFIX "chained_"
std::string getChainedKey(unsigned int msgId)
{
    return std::string(CHAINED_SET_PREFIX) + std::to_string(msgId);
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
        faabric::util::updateBatchExecAppId(req, msg.appid());
        faabric::util::updateBatchExecGroupId(req, msg.groupid());
        req->set_type(faabric::BatchExecuteRequest::MIGRATION);
        auto decision = planner::getPlannerClient().callFunctions(req);
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
