#pragma once

#include <catch2/catch.hpp>

#include "DummyExecutorFactory.h"
#include "faabric_utils.h"

#include <faabric/mpi/MpiWorld.h>
#include <faabric/mpi/MpiWorldRegistry.h>
#include <faabric/planner/PlannerClient.h>
#include <faabric/planner/PlannerServer.h>
#include <faabric/planner/planner.pb.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/redis/Redis.h>
#include <faabric/scheduler/ExecutorContext.h>
#include <faabric/scheduler/ExecutorFactory.h>
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
#include <faabric/util/json.h>
#include <faabric/util/latch.h>
#include <faabric/util/memory.h>
#include <faabric/util/network.h>
#include <faabric/util/scheduling.h>
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
      : decisionCache(faabric::util::getSchedulingDecisionCache())
    {}

    ~CachedDecisionTestFixture() { decisionCache.clear(); }

  protected:
    faabric::util::DecisionCache& decisionCache;
};

class PlannerClientServerFixture
{
  public:
    PlannerClientServerFixture()
      : plannerCli(LOCALHOST)
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
    faabric::planner::PlannerClient plannerCli;
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
            faabric::HostResources resources;
            resources.set_slots(slotsPerHost.at(i));
            resources.set_usedslots(usedPerHost.at(i));

            sch.addHostToGlobalSet(hosts.at(i));

            // If setting resources for the master host, update the scheduler.
            // Otherwise, queue the resource response
            if (i == 0) {
                sch.setThisHostResources(resources);
            } else {
                faabric::scheduler::queueResourceResponse(hosts.at(i),
                                                          resources);
            }
        }
    }

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
      , msg(faabric::util::messageFactory(user, func))
    {
        std::shared_ptr<faabric::scheduler::ExecutorFactory> fac =
          std::make_shared<faabric::scheduler::DummyExecutorFactory>();
        faabric::scheduler::setExecutorFactory(fac);

        msg.set_mpiworldid(worldId);
        msg.set_mpiworldsize(worldSize);
    }

    ~MpiBaseTestFixture()
    {
        // TODO - without this sleep, we sometimes clear the PTP broker before
        // all the executor threads have been set up, and when trying to query
        // for the comm. group we throw a runtime error.
        SLEEP_MS(200);
    }

  protected:
    const std::string user;
    const std::string func;
    int worldId;
    int worldSize;

    faabric::Message msg;
};

class MpiTestFixture : public MpiBaseTestFixture
{
  public:
    MpiTestFixture() { world.create(msg, worldId, worldSize); }

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

        // Set up the first world, holding the master rank (which already takes
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
        sch.addHostToGlobalSet(otherHost);

        // Queue the resource response for this other host
        faabric::scheduler::queueResourceResponse(otherHost, otherResources);
    }

  protected:
    std::string thisHost;
    std::string otherHost = LOCALHOST;

    std::shared_ptr<faabric::util::Latch> testLatch;

    faabric::mpi::MpiWorld otherWorld;
};
}
