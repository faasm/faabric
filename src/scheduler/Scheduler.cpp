#include <faabric/batch-scheduler/BatchScheduler.h>
#include <faabric/executor/ExecutorFactory.h>
#include <faabric/planner/PlannerClient.h>
#include <faabric/planner/planner.pb.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/FunctionCallClient.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/snapshot/SnapshotClient.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/transport/PointToPointBroker.h>
#include <faabric/util/batch.h>
#include <faabric/util/config.h>
#include <faabric/util/environment.h>
#include <faabric/util/func.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>
#include <faabric/util/snapshot.h>
#include <faabric/util/testing.h>

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
    // Start the reaper thread
    reaperThread.start(conf.reaperIntervalSeconds);
}

Scheduler::~Scheduler()
{
    if (!_isShutdown) {
        SPDLOG_ERROR("Destructing scheduler without shutting down first");
    }
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

    // Reset scheduler state
    threadResultMessages.clear();

    // Records
    recordedMessages.clear();

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
        std::vector<std::shared_ptr<faabric::executor::Executor>>& execs =
          execPair.second;
        std::vector<std::shared_ptr<faabric::executor::Executor>> toRemove;

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
    const std::string funcStr = faabric::util::funcToString(msg, true);
    return executors[funcStr].size();
}

void Scheduler::executeBatch(std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    faabric::util::FullLock lock(mx);

    bool isThreads = req->type() == faabric::BatchExecuteRequest::THREADS;
    auto funcStr = faabric::util::funcToString(req->messages(0), true);

    int nMessages = req->messages_size();

    // Records for tests - copy messages before execution to avoid races
    if (faabric::util::isTestMode()) {
        for (int i = 0; i < nMessages; i++) {
            recordedMessages.emplace_back(req->messages().at(i));
        }
    }

    // For threads we only need one executor, for anything else we want
    // one Executor per function in flight.
    if (isThreads) {
        // Threads use the existing executor. There must only be one at a time,
        // thus we use a function string that includes the app id
        std::vector<std::shared_ptr<faabric::executor::Executor>>&
          thisExecutors = executors[funcStr];

        std::shared_ptr<faabric::executor::Executor> e = nullptr;
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
        std::vector<int> thisHostIdxs(req->messages_size());
        std::iota(thisHostIdxs.begin(), thisHostIdxs.end(), 0);
        e->executeTasks(thisHostIdxs, req);
    } else {
        // Non-threads require one executor per task
        for (int i = 0; i < nMessages; i++) {
            faabric::Message& localMsg = req->mutable_messages()->at(i);

            try {
                std::shared_ptr<faabric::executor::Executor> exec =
                  claimExecutor(localMsg, lock);
                exec->executeTasks({ i }, req);
            } catch (std::exception& exc) {
                SPDLOG_ERROR(
                  "{}:{}:{} Error trying to claim executor for message {}",
                  localMsg.appid(),
                  localMsg.groupid(),
                  localMsg.groupidx(),
                  localMsg.id());

                // Even if the task is not executed, set its result so that
                // everybody knows it has failed
                localMsg.set_returnvalue(1);
                localMsg.set_outputdata("Error trying to claim executor");
                faabric::planner::getPlannerClient().setMessageResult(
                  std::make_shared<faabric::Message>(localMsg));
            }
        }
    }
}

void Scheduler::clearRecordedMessages()
{
    faabric::util::FullLock lock(mx);
    recordedMessages.clear();
}

std::vector<faabric::Message> Scheduler::getRecordedMessages()
{
    faabric::util::SharedLock lock(mx);
    return recordedMessages;
}

std::shared_ptr<faabric::executor::Executor> Scheduler::claimExecutor(
  faabric::Message& msg,
  faabric::util::FullLock& schedulerLock)
{
    std::string funcStr = faabric::util::funcToString(msg, true);

    std::vector<std::shared_ptr<faabric::executor::Executor>>& thisExecutors =
      executors[funcStr];

    auto factory = faabric::executor::getExecutorFactory();

    std::shared_ptr<faabric::executor::Executor> claimed = nullptr;
    for (auto& e : thisExecutors) {
        if (e->tryClaim()) {
            claimed = e;
            // Reset the just claimed warm executor to guarantee TLS is
            // refreshed
            claimed->reset(msg);
            SPDLOG_DEBUG("Reusing warm executor {} for {} (idx: {} - rank: {})",
                         claimed->id,
                         funcStr,
                         msg.groupidx(),
                         msg.mpirank());
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

void Scheduler::setThreadResultLocally(uint32_t appId,
                                       uint32_t msgId,
                                       int32_t returnValue,
                                       faabric::transport::Message& message)
{
    // Keep the message
    faabric::util::FullLock lock(mx);
    threadResultMessages.insert(std::make_pair(msgId, std::move(message)));
}

// TODO(scheduler-cleanup): move method elsewhere
std::vector<std::pair<uint32_t, int32_t>> Scheduler::awaitThreadResults(
  std::shared_ptr<faabric::BatchExecuteRequest> req,
  int timeoutMs)
{
    std::vector<std::pair<uint32_t, int32_t>> results;
    results.reserve(req->messages_size());
    for (int i = 0; i < req->messages_size(); i++) {
        uint32_t messageId = req->messages().at(i).id();

        auto msgResult = faabric::planner::getPlannerClient().getMessageResult(
          req->appid(), messageId, timeoutMs);
        results.emplace_back(messageId, msgResult.returnvalue());
    }

    return results;
}

size_t Scheduler::getCachedMessageCount()
{
    return threadResultMessages.size();
}

void Scheduler::setThisHostResources(faabric::HostResources& res)
{
    addHostToGlobalSet(thisHost, std::make_shared<faabric::HostResources>(res));
    conf.overrideCpuCount = res.slots();
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
        // To check for migration opportunities, we request a scheduling
        // decision for the same batch execute request, but setting the
        // migration flag
        auto req =
          faabric::util::batchExecFactory(msg.user(), msg.function(), 1);
        faabric::util::updateBatchExecAppId(req, msg.appid());
        faabric::util::updateBatchExecGroupId(req, msg.groupid());
        req->set_type(faabric::BatchExecuteRequest::MIGRATION);
        auto decision = planner::getPlannerClient().callFunctions(req);

        // Update the group ID if we want to migrate
        if (decision == DO_NOT_MIGRATE_DECISION) {
            newGroupId = groupId;
        } else if (decision == MUST_FREEZE_DECISION) {
            newGroupId = MUST_FREEZE;
        } else {
            newGroupId = decision.groupId;
        }

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
        // we can set it here (and in fact, we need to do so when faking two
        // hosts)
        newGroupId = overwriteNewGroupId;
    }

    if (newGroupId == MUST_FREEZE) {
        auto migration = std::make_shared<faabric::PendingMigration>();
        migration->set_appid(MUST_FREEZE);

        return migration;
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
