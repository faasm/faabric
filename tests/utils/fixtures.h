#pragma once

#include <catch2/catch.hpp>

#include "DummyExecutorFactory.h"
#include "faabric_utils.h"

#include "DummyExecutorFactory.h"
#include "faabric_utils.h"

#include <faabric/planner/PlannerClient.h>
#include <faabric/planner/planner.pb.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/redis/Redis.h>
#include <faabric/scheduler/ExecutorContext.h>
#include <faabric/scheduler/ExecutorFactory.h>
#include <faabric/scheduler/FunctionCallServer.h>
#include <faabric/scheduler/MpiWorld.h>
#include <faabric/scheduler/MpiWorldRegistry.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/state/InMemoryStateKeyValue.h>
#include <faabric/state/State.h>
#include <faabric/transport/PointToPointBroker.h>
#include <faabric/transport/PointToPointClient.h>
#include <faabric/transport/PointToPointServer.h>
#include <faabric/util/dirty.h>
#include <faabric/util/func.h>
#include <faabric/util/gids.h>
#include <faabric/util/json.h>
#include <faabric/util/latch.h>
#include <faabric/util/memory.h>
#include <faabric/util/network.h>
#include <faabric/util/scheduling.h>
#include <faabric/util/testing.h>

#include <sys/mman.h>

namespace tests {
class RedisTestFixture
{
  public:
    RedisTestFixture()
      : redis(faabric::redis::Redis::getQueue())
    {
        redis.flushAll();
    }
    ~RedisTestFixture() { redis.flushAll(); }

  protected:
    faabric::redis::Redis& redis;
};

class StateTestFixture
{
  public:
    StateTestFixture()
      : state(faabric::state::getGlobalState())
    {
        doCleanUp();
    }

    ~StateTestFixture() { doCleanUp(); }

  protected:
    faabric::state::State& state;
    void doCleanUp()
    {
        // Clear out any cached state, do so for both modes
        faabric::util::SystemConfig& conf = faabric::util::getSystemConfig();
        std::string& originalStateMode = conf.stateMode;
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
      : decisionCache(faabric::util::getSchedulingDecisionCache())
    {}

    ~CachedDecisionTestFixture() { decisionCache.clear(); }

  protected:
    faabric::util::DecisionCache& decisionCache;
};

class PlannerTestFixture
{
  public:
    PlannerTestFixture()
    {
        // Ensure the server is reachable
        plannerCli.ping();
    }

    ~PlannerTestFixture() { resetPlanner(); }

  protected:
    // TODO: re-factor to plannerCli
    faabric::planner::PlannerClient plannerCli;

    void resetPlanner() const
    {
        faabric::planner::HttpMessage msg;
        msg.set_type(faabric::planner::HttpMessage_Type_RESET);
        std::string jsonStr = faabric::util::messageToJson(msg);

        faabric::util::SystemConfig& conf = faabric::util::getSystemConfig();
        std::pair<int, std::string> result =
          postToUrl(conf.plannerHost, conf.plannerPort, jsonStr);
        assert(result.first == 200);
    }

    void setPlannerMockedHosts(const std::vector<std::string>& hosts)
    {
        faabric::planner::PlannerTestsConfig testsConfig;
        for (const auto& host : hosts) {
            testsConfig.add_mockedhosts(host);
        }
        plannerCli.setTestsConfig(testsConfig);
    }

    faabric::planner::PlannerConfig getPlannerConfig()
    {
        faabric::planner::HttpMessage msg;
        msg.set_type(faabric::planner::HttpMessage_Type_GET_CONFIG);
        std::string jsonStr = faabric::util::messageToJson(msg);

        faabric::util::SystemConfig& conf = faabric::util::getSystemConfig();
        std::pair<int, std::string> result =
          postToUrl(conf.plannerHost, conf.plannerPort, jsonStr);
        REQUIRE(result.first == 200);

        // Check that we can de-serialise the config. Note that if there's a
        // de-serialisation the method will throw an exception
        faabric::planner::PlannerConfig config;
        faabric::util::jsonToMessage(result.second, &config);
        return config;
    }

    void flushExecutors()
    {
        faabric::planner::HttpMessage msg;
        msg.set_type(faabric::planner::HttpMessage_Type_FLUSH_EXECUTORS);
        std::string jsonStr = faabric::util::messageToJson(msg);

        faabric::util::SystemConfig& conf = faabric::util::getSystemConfig();
        std::pair<int, std::string> result =
          postToUrl(conf.plannerHost, conf.plannerPort, jsonStr);
        REQUIRE(result.first == 200);
    }
};

class FunctionCallServerTestFixture
{
  public:
    FunctionCallServerTestFixture()
    {
        executorFactory =
          std::make_shared<faabric::scheduler::DummyExecutorFactory>();
        setExecutorFactory(executorFactory);

        functionCallServer.start();
    }

    ~FunctionCallServerTestFixture()
    {
        functionCallServer.stop();
        executorFactory->reset();
    }

  protected:
    faabric::scheduler::FunctionCallServer functionCallServer;
    std::shared_ptr<faabric::scheduler::DummyExecutorFactory> executorFactory;
};

class SchedulerTestFixture
  : public CachedDecisionTestFixture
  , public PlannerTestFixture
// TODO: right-now, scheduler's callFunctions depends on the function call
// server being online
// , public FunctionCallServerTestFixture
{
  public:
    SchedulerTestFixture()
      : sch(faabric::scheduler::getScheduler())
    {
        faabric::util::setMockMode(false);
        faabric::util::setTestMode(true);

        faabric::scheduler::clearMockRequests();
        faabric::snapshot::clearMockSnapshotRequests();

        sch.shutdown();
        // Make sure we have enough space in the scheduler for the tests
        auto thisHostResources = std::make_shared<faabric::HostResources>();
        thisHostResources->set_slots(20);
        thisHostResources->set_usedslots(0);
        sch.addHostToGlobalSet(faabric::util::getSystemConfig().endpointHost,
                               thisHostResources);
    };

    ~SchedulerTestFixture()
    {
        faabric::util::setMockMode(false);
        faabric::util::setTestMode(true);

        faabric::scheduler::clearMockRequests();
        faabric::snapshot::clearMockSnapshotRequests();

        sch.shutdown();
        sch.addHostToGlobalSet();

        faabric::util::getDirtyTracker()->clearAll();
    };

    // Helper method to set the available hosts and slots per host prior to
    // making a scheduling decision
    void setHostResources(std::vector<std::string> hosts,
                          std::vector<int> slotsPerHost,
                          std::vector<int> usedPerHost)
    {
        if (hosts.size() != slotsPerHost.size() ||
            hosts.size() != usedPerHost.size()) {
            SPDLOG_ERROR("Must provide one value for slots and used per host");
            throw std::runtime_error(
              "Not providing one value per slot and used per host");
        }

        sch.clearRecordedMessages();
        faabric::scheduler::clearMockRequests();

        for (int i = 0; i < hosts.size(); i++) {
            auto resources = std::make_shared<faabric::HostResources>();
            resources->set_slots(slotsPerHost.at(i));
            resources->set_usedslots(usedPerHost.at(i));

            sch.addHostToGlobalSet(hosts.at(i), resources);
        }
    }

  protected:
    faabric::scheduler::Scheduler& sch;
};

class SnapshotTestFixture
{
  public:
    SnapshotTestFixture()
      : reg(faabric::snapshot::getSnapshotRegistry())
    {
        reg.clear();
    }

    ~SnapshotTestFixture()
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

class ConfTestFixture
{
  public:
    ConfTestFixture()
      : conf(faabric::util::getSystemConfig()){};

    ~ConfTestFixture() { conf.reset(); };

  protected:
    faabric::util::SystemConfig& conf;
};

class PointToPointTestFixture
{
  public:
    PointToPointTestFixture()
      : broker(faabric::transport::getPointToPointBroker())
    {
        faabric::util::setMockMode(false);
        broker.clear();
    }

    ~PointToPointTestFixture()
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

class PointToPointClientServerFixture : public PointToPointTestFixture
{
  public:
    PointToPointClientServerFixture()
      : cli(LOCALHOST)
    {
        server.start();
    }

    ~PointToPointClientServerFixture() { server.stop(); }

  protected:
    faabric::transport::PointToPointClient cli;
    faabric::transport::PointToPointServer server;
};

class MpiBaseTestFixture
  : public SchedulerTestFixture
  , public PointToPointClientServerFixture
  , public ConfTestFixture
  , public FunctionCallServerTestFixture
{
  public:
    MpiBaseTestFixture()
      : user("mpi")
      , func("hellompi")
      , worldSize(5)
      , req(faabric::util::batchExecFactory(user, func, 1))
      , msg(*req->mutable_messages(0))
    {
        std::shared_ptr<faabric::scheduler::ExecutorFactory> fac =
          std::make_shared<faabric::scheduler::DummyExecutorFactory>();
        faabric::scheduler::setExecutorFactory(fac);

        auto& mpiRegistry = faabric::scheduler::getMpiWorldRegistry();
        mpiRegistry.clear();

        msg.set_ismpi(true);
        msg.set_mpiworldid(msg.appid());
        msg.set_mpiworldsize(worldSize);

        // Register the first message that creates the world size
        sch.callFunctions(req);
    }

    ~MpiBaseTestFixture()
    {
        // TODO - without this sleep, we sometimes clear the PTP broker before
        // all the executor threads have been set up, and when trying to query
        // for the comm. group we throw a runtime error.
        SLEEP_MS(200);
        auto& mpiRegistry = faabric::scheduler::getMpiWorldRegistry();
        mpiRegistry.clear();
    }

  protected:
    const std::string user;
    const std::string func;
    int worldId;
    int worldSize;

    std::shared_ptr<faabric::BatchExecuteRequest> req;
    faabric::Message& msg;
};

class MpiTestFixture : public MpiBaseTestFixture
{
  public:
    MpiTestFixture() { world.create(msg, worldId, worldSize); }

    ~MpiTestFixture() { world.destroy(); }

  protected:
    faabric::scheduler::MpiWorld world;
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

        faabric::scheduler::getMpiWorldRegistry().clear();
    }

    void setWorldSizes(int worldSize, int ranksThisWorld, int ranksOtherWorld)
    {
        // The MpiBaseTestFixture calls the message in its constructor to make
        // sure the world can be created. Here we want to start from scratch,
        // so we create a "new" request (by updating the app id) and update
        // the host resources
        req->set_appid(faabric::util::generateGid());
        msg.set_appid(req->appid());
        msg.set_mpiworldsize(worldSize);

        // Set up this host resources
        faabric::HostResources thisResources;
        thisResources.set_slots(ranksThisWorld);
        thisResources.set_usedslots(0);
        sch.addHostToGlobalSet(
          thisHost, std::make_shared<faabric::HostResources>(thisResources));

        // Call the request _before_ setting up the second host, to make sure
        // the request gets scheduled to this host
        sch.callFunctions(req);

        // Set up the other world and add it to the global set of hosts
        faabric::HostResources otherResources;
        otherResources.set_slots(ranksOtherWorld);
        thisResources.set_usedslots(0);
        sch.addHostToGlobalSet(
          otherHost, std::make_shared<faabric::HostResources>(otherResources));

        // Add the other world to the list of mocked hosts
        setPlannerMockedHosts({ otherHost });

        // Queue the resource response for this other host
        // faabric::scheduler::queueResourceResponse(otherHost, otherResources);
    }

  protected:
    std::string thisHost;
    std::string otherHost = LOCALHOST;

    std::shared_ptr<faabric::util::Latch> testLatch;

    faabric::scheduler::MpiWorld otherWorld;
};

class ExecutorContextTestFixture
{
  public:
    ExecutorContextTestFixture() {}

    ~ExecutorContextTestFixture()
    {
        faabric::scheduler::ExecutorContext::unset();
    }

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

class DirtyTrackingTestFixture : public ConfTestFixture
{
  public:
    DirtyTrackingTestFixture()
    {
        conf.reset();
        faabric::util::resetDirtyTracker();
    };

    ~DirtyTrackingTestFixture()
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
}
