#pragma once

#include <catch2/catch.hpp>

#include "DummyExecutorFactory.h"
#include "faabric_utils.h"

#include <faabric/batch-scheduler/BatchScheduler.h>
#include <faabric/batch-scheduler/BinPackScheduler.h>
#include <faabric/batch-scheduler/DecisionCache.h>
#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/mpi/MpiWorld.h>
#include <faabric/mpi/MpiWorldRegistry.h>
#include <faabric/planner/PlannerClient.h>
#include <faabric/planner/PlannerServer.h>
#include <faabric/planner/planner.pb.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/redis/Redis.h>
#include <faabric/scheduler/ExecutorContext.h>
#include <faabric/scheduler/ExecutorFactory.h>
#include <faabric/scheduler/FunctionCallClient.h>
#include <faabric/scheduler/FunctionCallServer.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/state/InMemoryStateKeyValue.h>
#include <faabric/state/State.h>
#include <faabric/transport/PointToPointBroker.h>
#include <faabric/transport/PointToPointClient.h>
#include <faabric/transport/PointToPointServer.h>
#include <faabric/util/batch.h>
#include <faabric/util/dirty.h>
#include <faabric/util/environment.h>
#include <faabric/util/gids.h>
#include <faabric/util/json.h>
#include <faabric/util/latch.h>
#include <faabric/util/memory.h>
#include <faabric/util/network.h>
#include <faabric/util/testing.h>

#include <sys/mman.h>

// This file contains the common test fixtures used throughout the tests. A
// test fixture is the mocking of a component for the purpose of testing it.
// To that extent, fixtures that are meant to be shared (i.e. included in this
// file) should aim to be as concise as possible, and include the minimum
// amount of dependencies (in therms of parent classes) to mimick the
// corresponding component. Complex and attribute-rich features should only
// be defined in test files. To differentiate the two, we name
// <ComponentName>Fixture those simple, concise, fixtures that mimick one
// component, and <Component>TestFixture for the attribute rich ones.
// Note that most of the features included in this file are also used in
// Faasm.

namespace tests {
class RedisFixture
{
  public:
    RedisFixture()
      : redis(faabric::redis::Redis::getQueue())
    {
        redis.flushAll();
    }
    ~RedisFixture() { redis.flushAll(); }

  protected:
    faabric::redis::Redis& redis;
};

class StateFixture
{
  public:
    StateFixture()
      : state(faabric::state::getGlobalState())
    {
        doCleanUp();
    }

    ~StateFixture() { doCleanUp(); }

  protected:
    faabric::state::State& state;
    std::string oldStateMode;

    void setUpStateMode(const std::string& stateMode)
    {
        faabric::util::SystemConfig& conf = faabric::util::getSystemConfig();
        oldStateMode = conf.stateMode;
        conf.stateMode = stateMode;
    }

    void doCleanUp()
    {
        // Clear out any cached state, do so for both modes
        faabric::util::SystemConfig& conf = faabric::util::getSystemConfig();
        std::string& originalStateMode =
          oldStateMode.empty() ? conf.stateMode : oldStateMode;
        conf.stateMode = "inmemory";
        state.forceClearAll(true);
        conf.stateMode = "redis";
        state.forceClearAll(true);
        conf.stateMode = originalStateMode;
    }
};

class CachedDecisionTestFixture
{
  public:
    CachedDecisionTestFixture()
      : decisionCache(faabric::batch_scheduler::getSchedulingDecisionCache())
    {}

    ~CachedDecisionTestFixture() { decisionCache.clear(); }

  protected:
    faabric::batch_scheduler::DecisionCache& decisionCache;
};

class PlannerClientServerFixture
{
  public:
    PlannerClientServerFixture()
      : plannerCli(faabric::planner::getPlannerClient())
    {
        plannerServer.start();
        plannerCli.ping();
    }

    ~PlannerClientServerFixture()
    {
        plannerServer.stop();
        faabric::planner::getPlanner().reset();
    }

  protected:
    faabric::planner::PlannerClient& plannerCli;
    faabric::planner::PlannerServer plannerServer;
};

class SchedulerFixture
  // We need to mock the planner server every time we mock the scheduler
  // because the planner server handles host membership calls, and in turn
  // the scheduler's add/remove host from global set
  : public PlannerClientServerFixture
{
  public:
    SchedulerFixture()
      : sch(faabric::scheduler::getScheduler())
    {
        faabric::util::setMockMode(false);
        faabric::util::setTestMode(true);

        faabric::scheduler::clearMockRequests();
        faabric::snapshot::clearMockSnapshotRequests();

        sch.shutdown();
        sch.addHostToGlobalSet();
    };

    ~SchedulerFixture()
    {
        faabric::util::setMockMode(false);
        faabric::util::setTestMode(true);

        faabric::scheduler::clearMockRequests();
        faabric::snapshot::clearMockSnapshotRequests();

        sch.shutdown();
        sch.addHostToGlobalSet();

        faabric::util::getDirtyTracker()->clearAll();
    };

  protected:
    faabric::scheduler::Scheduler& sch;
};

class SnapshotRegistryFixture
{
  public:
    SnapshotRegistryFixture()
      : reg(faabric::snapshot::getSnapshotRegistry())
    {
        reg.clear();
    }

    ~SnapshotRegistryFixture()
    {
        reg.clear();
        faabric::util::getDirtyTracker()->clearAll();
    }

    std::shared_ptr<faabric::util::SnapshotData> setUpSnapshot(
      const std::string& snapKey,
      int nPages)
    {
        size_t snapSize = nPages * faabric::util::HOST_PAGE_SIZE;
        auto snapData = std::make_shared<faabric::util::SnapshotData>(snapSize);
        reg.registerSnapshot(snapKey, snapData);

        return snapData;
    }

    void removeSnapshot(const std::string& key, int nPages)
    {
        auto snap = reg.getSnapshot(key);
        reg.deleteSnapshot(key);
    }

  protected:
    faabric::snapshot::SnapshotRegistry& reg;
};

class ConfFixture
{
  public:
    ConfFixture()
      : conf(faabric::util::getSystemConfig()){};

    ~ConfFixture() { conf.reset(); };

  protected:
    faabric::util::SystemConfig& conf;
};

class PointToPointBrokerFixture
{
  public:
    PointToPointBrokerFixture()
      : broker(faabric::transport::getPointToPointBroker())
    {
        faabric::util::setMockMode(false);
        broker.clear();
    }

    ~PointToPointBrokerFixture()
    {
        // Here we reset the thread-local cache for the test thread. If other
        // threads are used in the tests, they too must do this.
        broker.resetThreadLocalCache();

        faabric::transport::clearSentMessages();

        broker.clear();
        faabric::util::setMockMode(false);
    }

  protected:
    faabric::transport::PointToPointBroker& broker;
};

class PointToPointClientServerFixture
  // To mock the P2P client/server we need to mock the PTP broker first
  : public PointToPointBrokerFixture
{
  public:
    PointToPointClientServerFixture()
      : ptpClient(LOCALHOST)
    {
        ptpServer.start();
    }

    ~PointToPointClientServerFixture() { ptpServer.stop(); }

  protected:
    faabric::transport::PointToPointClient ptpClient;
    faabric::transport::PointToPointServer ptpServer;
};

class ExecutorContextFixture
{
  public:
    ExecutorContextFixture() {}

    ~ExecutorContextFixture() { faabric::scheduler::ExecutorContext::unset(); }

    /**
     * Creates a batch request and sets up the associated context
     */
    std::shared_ptr<faabric::BatchExecuteRequest> setUpContext(
      const std::string& user,
      const std::string& func,
      int nMsgs = 1)
    {
        auto req = faabric::util::batchExecFactory(user, func, nMsgs);

        setUpContext(req);

        return req;
    }

    /**
     * Sets up context for the given batch request
     */
    void setUpContext(std::shared_ptr<faabric::BatchExecuteRequest> req)
    {
        faabric::scheduler::ExecutorContext::set(nullptr, req, 0);
    }
};

#define TEST_EXECUTOR_DEFAULT_MEMORY_SIZE (10 * faabric::util::HOST_PAGE_SIZE)

class TestExecutor final : public faabric::scheduler::Executor
{
  public:
    TestExecutor(faabric::Message& msg);

    faabric::util::MemoryRegion dummyMemory = nullptr;
    size_t dummyMemorySize = TEST_EXECUTOR_DEFAULT_MEMORY_SIZE;
    size_t maxMemorySize = 0;

    void reset(faabric::Message& msg) override;

    void restore(const std::string& snapshotKey) override;

    std::span<uint8_t> getMemoryView() override;

    void setUpDummyMemory(size_t memSize);

    size_t getMaxMemorySize() override;

    int32_t executeTask(
      int threadPoolIdx,
      int msgIdx,
      std::shared_ptr<faabric::BatchExecuteRequest> reqOrig) override;
};

class TestExecutorFactory : public faabric::scheduler::ExecutorFactory
{
  protected:
    std::shared_ptr<faabric::scheduler::Executor> createExecutor(
      faabric::Message& msg) override;
};

class DirtyTrackingFixture : public ConfFixture
{
  public:
    DirtyTrackingFixture()
    {
        conf.reset();
        faabric::util::resetDirtyTracker();
    };

    ~DirtyTrackingFixture()
    {
        faabric::util::getDirtyTracker()->clearAll();
        conf.reset();
        faabric::util::resetDirtyTracker();
    }

    void setTrackingMode(const std::string& mode)
    {
        conf.dirtyTrackingMode = mode;
        faabric::util::resetDirtyTracker();
    }
};

class FunctionCallClientServerFixture
{
  protected:
    faabric::scheduler::FunctionCallServer functionCallServer;
    faabric::scheduler::FunctionCallClient functionCallClient;

  public:
    FunctionCallClientServerFixture()
      : functionCallClient(LOCALHOST)
    {
        functionCallServer.start();
    }

    ~FunctionCallClientServerFixture() { functionCallServer.stop(); }
};

class MpiWorldRegistryFixture
{
  public:
    MpiWorldRegistryFixture()
      : mpiRegistry(faabric::mpi::getMpiWorldRegistry())
    {
        mpiRegistry.clear();
    }

    ~MpiWorldRegistryFixture() { mpiRegistry.clear(); }

  protected:
    faabric::mpi::MpiWorldRegistry& mpiRegistry;
};

class MpiBaseTestFixture
  : public FunctionCallClientServerFixture
  , public MpiWorldRegistryFixture
  , public SchedulerFixture
{
  public:
    MpiBaseTestFixture()
      : user("mpi")
      , func("hellompi")
      , worldId(123)
      , worldSize(5)
      , req(faabric::util::batchExecFactory(user, func, 1))
      , msg(*req->mutable_messages(0))
    {
        std::shared_ptr<faabric::scheduler::ExecutorFactory> fac =
          std::make_shared<faabric::scheduler::DummyExecutorFactory>();
        faabric::scheduler::setExecutorFactory(fac);

        msg.set_mpiworldid(worldId);
        msg.set_mpiworldsize(worldSize);

        // Make enough space in this host to run MPI functions
        faabric::HostResources res;
        res.set_slots(2 * worldSize);
        sch.setThisHostResources(res);

        // Call the request, so that we have the original message recorded
        // in the planner
        // plannerCli.callFunctions(req);
    }

    ~MpiBaseTestFixture()
    {
        // Make sure we get the message result to avoid data races
        plannerCli.getMessageResult(msg, 500);
    }

  protected:
    const std::string user;
    const std::string func;
    int worldId;
    int worldSize;

    std::shared_ptr<BatchExecuteRequest> req;
    faabric::Message& msg;

    // This method waits for all MPI messages to be scheduled. In MPI,
    // (worldSize - 1) messages are scheduled after calling MpiWorld::create.
    // Thus, it is hard when this second batch has already started executing
    void waitForMpiMessages(
      std::shared_ptr<BatchExecuteRequest> reqIn = nullptr,
      int expectedWorldSize = 0) const
    {
        if (reqIn == nullptr) {
            reqIn = req;
        }

        if (expectedWorldSize == 0) {
            expectedWorldSize = worldSize;
        }

        int maxRetries = 5;
        int numRetries = 0;
        auto decision = plannerCli.getSchedulingDecision(reqIn);
        while (decision.messageIds.size() != expectedWorldSize) {
            if (numRetries >= maxRetries) {
                SPDLOG_ERROR(
                  "Timed-out waiting for MPI messages to be scheduled ({}/{})",
                  decision.messageIds.size(),
                  expectedWorldSize);
                throw std::runtime_error("Timed-out waiting for MPI messges");
            }

            SPDLOG_DEBUG(
              "Waiting for MPI messages to be scheduled (app: {} - {}/{})",
              reqIn->appid(),
              decision.messageIds.size(),
              expectedWorldSize);
            SLEEP_MS(200);

            numRetries += 1;
            decision = plannerCli.getSchedulingDecision(reqIn);

            // If the decision has no app ID, it means that the app has
            // already finished, so we don't even have to wait for the messages
            if (decision.appId == 0) {
                return;
            }
        }

        for (auto mid : decision.messageIds) {
            plannerCli.getMessageResult(decision.appId, mid, 500);
        }
    }
};

class MpiTestFixture : public MpiBaseTestFixture
{
  public:
    MpiTestFixture()
    {
        plannerCli.callFunctions(req);
        world.create(msg, worldId, worldSize);
    }

    ~MpiTestFixture() { world.destroy(); }

  protected:
    faabric::mpi::MpiWorld world;
};

// Note that this test has two worlds, which each "think" that the other is
// remote. This is done by allowing one to have the IP of this host, the other
// to have the localhost IP, i.e. 127.0.0.1.
// This fixture must only be used in mocking mode. To test a real MPI execution
// across different hosts you must write a distributed test.
class RemoteMpiTestFixture : public MpiBaseTestFixture
{
  public:
    RemoteMpiTestFixture()
      : thisHost(faabric::util::getSystemConfig().endpointHost)
      , testLatch(faabric::util::Latch::create(2))
    {
        otherWorld.overrideHost(otherHost);

        faabric::util::setMockMode(true);
    }

    ~RemoteMpiTestFixture()
    {
        faabric::util::setMockMode(false);

        faabric::mpi::getMpiWorldRegistry().clear();
    }

    void setWorldSizes(int worldSize, int ranksThisWorld, int ranksOtherWorld)
    {
        // Update message
        msg.set_mpiworldsize(worldSize);
        plannerCli.callFunctions(req);

        // Set up the first world, holding the main rank (which already takes
        // one slot).
        // Note that any excess ranks will also be allocated to this world when
        // the scheduler is overloaded.
        faabric::HostResources thisResources;
        thisResources.set_slots(ranksThisWorld);
        thisResources.set_usedslots(1);
        sch.setThisHostResources(thisResources);

        // Set up the other world and add it to the global set of hosts
        faabric::HostResources otherResources;
        otherResources.set_slots(ranksOtherWorld);
        sch.addHostToGlobalSet(
          otherHost, std::make_shared<faabric::HostResources>(otherResources));
    }

  protected:
    std::string thisHost;
    std::string otherHost = LOCALHOST;

    std::shared_ptr<faabric::util::Latch> testLatch;

    faabric::mpi::MpiWorld otherWorld;
};

class BatchSchedulerFixture : public ConfFixture
{
  public:
    BatchSchedulerFixture()
      : appId(faabric::util::generateGid())
      , groupId(0)
      , actualDecision(appId, groupId)
    {}

    ~BatchSchedulerFixture()
    {
        faabric::batch_scheduler::resetBatchScheduler();
    }

  protected:
    int appId;
    int groupId;

    std::shared_ptr<BatchExecuteRequest> ber;
    std::shared_ptr<faabric::batch_scheduler::BatchScheduler> batchScheduler;
    faabric::batch_scheduler::SchedulingDecision actualDecision;

    struct BatchSchedulerConfig
    {
        faabric::batch_scheduler::HostMap hostMap;
        faabric::batch_scheduler::InFlightReqs inFlightReqs;
        faabric::batch_scheduler::SchedulingDecision expectedDecision;
    };

    static faabric::batch_scheduler::HostMap buildHostMap(
      std::vector<std::string> ips,
      std::vector<int> slots,
      std::vector<int> usedSlots)
    {
        faabric::batch_scheduler::HostMap hostMap;

        assert(ips.size() == slots.size());
        assert(slots.size() == usedSlots.size());

        for (int i = 0; i < ips.size(); i++) {
            hostMap[ips.at(i)] =
              std::make_shared<faabric::batch_scheduler::HostState>(
                ips.at(i), slots.at(i), usedSlots.at(i));
        }

        return hostMap;
    }

    static faabric::batch_scheduler::InFlightReqs buildInFlightReqs(
      std::shared_ptr<BatchExecuteRequest> ber,
      int numMsgsOldBer,
      std::vector<std::string> hosts)
    {
        faabric::batch_scheduler::InFlightReqs inFlightReqs;
        int appId = ber->appid();

        std::shared_ptr<BatchExecuteRequest> oldBer = nullptr;
        // If possible, literally copy the messages from the new BER to the
        // old one (the one in-flight)
        if (numMsgsOldBer > ber->messages_size()) {
            oldBer =
              faabric::util::batchExecFactory(ber->messages(0).user(),
                                              ber->messages(0).function(),
                                              numMsgsOldBer);
        } else {
            oldBer = faabric::util::batchExecFactory(
              ber->messages(0).user(), ber->messages(0).function(), 0);
            for (int i = 0; i < numMsgsOldBer; i++) {
                *oldBer->add_messages() = *ber->mutable_messages(i);
            }
        }
        oldBer->set_appid(appId);

        assert(oldBer->messages_size() == hosts.size());
        inFlightReqs[appId] = std::make_pair(
          oldBer,
          std::make_shared<faabric::batch_scheduler::SchedulingDecision>(
            buildExpectedDecision(oldBer, hosts)));

        return inFlightReqs;
    }

    static faabric::batch_scheduler::SchedulingDecision buildExpectedDecision(
      std::shared_ptr<BatchExecuteRequest> ber,
      std::vector<std::string> hosts)
    {
        faabric::batch_scheduler::SchedulingDecision decision(ber->appid(), 0);

        assert(ber->messages_size() == hosts.size());

        for (int i = 0; i < hosts.size(); i++) {
            decision.addMessage(hosts.at(i), ber->messages(i));
        }

        return decision;
    }

    static void compareSchedulingDecisions(
      const faabric::batch_scheduler::SchedulingDecision& decisionA,
      const faabric::batch_scheduler::SchedulingDecision& decisionB)
    {
        REQUIRE(decisionA.appId == decisionB.appId);
        REQUIRE(decisionA.groupId == decisionB.groupId);
        REQUIRE(decisionA.nFunctions == decisionB.nFunctions);
        REQUIRE(decisionA.hosts == decisionB.hosts);
        REQUIRE(decisionA.messageIds == decisionB.messageIds);
        REQUIRE(decisionA.appIdxs == decisionB.appIdxs);
        REQUIRE(decisionA.groupIdxs == decisionB.groupIdxs);
    }
};
}
