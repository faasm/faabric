#pragma once

#include <sys/mman.h>

#include <faabric/redis/Redis.h>
#include <faabric/scheduler/ExecutorFactory.h>
#include <faabric/scheduler/MpiWorld.h>
#include <faabric/scheduler/MpiWorldRegistry.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/state/InMemoryStateKeyValue.h>
#include <faabric/state/State.h>
#include <faabric/transport/PointToPointBroker.h>
#include <faabric/transport/PointToPointClient.h>
#include <faabric/transport/PointToPointServer.h>
#include <faabric/util/latch.h>
#include <faabric/util/memory.h>
#include <faabric/util/network.h>
#include <faabric/util/testing.h>

#include "DummyExecutorFactory.h"

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

class SchedulerTestFixture
{
  public:
    SchedulerTestFixture()
      : sch(faabric::scheduler::getScheduler())
    {
        faabric::util::resetDirtyTracking();

        faabric::util::setMockMode(false);
        faabric::util::setTestMode(true);

        faabric::scheduler::clearMockRequests();
        faabric::snapshot::clearMockSnapshotRequests();

        sch.shutdown();
        sch.addHostToGlobalSet();
    };

    ~SchedulerTestFixture()
    {
        faabric::util::setMockMode(false);
        faabric::util::setTestMode(true);

        faabric::scheduler::clearMockRequests();
        faabric::snapshot::clearMockSnapshotRequests();

        sch.shutdown();
        sch.addHostToGlobalSet();

        faabric::util::resetDirtyTracking();
    };

  protected:
    faabric::scheduler::Scheduler& sch;
};

class SnapshotTestFixture
{
  public:
    SnapshotTestFixture()
      : reg(faabric::snapshot::getSnapshotRegistry())
    {
        faabric::util::resetDirtyTracking();
        reg.clear();
    }

    ~SnapshotTestFixture()
    {
        faabric::util::resetDirtyTracking();
        reg.clear();
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

class MpiBaseTestFixture
  : public SchedulerTestFixture
  , public ConfTestFixture
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

        auto& mpiRegistry = faabric::scheduler::getMpiWorldRegistry();
        mpiRegistry.clear();

        msg.set_mpiworldid(worldId);
        msg.set_mpiworldsize(worldSize);
    }

    ~MpiBaseTestFixture()
    {
        auto& mpiRegistry = faabric::scheduler::getMpiWorldRegistry();
        mpiRegistry.clear();
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
    faabric::scheduler::MpiWorld world;
};

// Note that this test has two worlds, which each "think" that the other is
// remote. This is done by allowing one to have the IP of this host, the other
// to have the localhost IP, i.e. 127.0.0.1.
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

    faabric::scheduler::MpiWorld otherWorld;
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
        // Note - here we reset the thread-local cache for the test thread. If
        // other threads are used in the tests, they too must do this.
        broker.resetThreadLocalCache();

        faabric::transport::clearSentMessages();

        broker.clear();
        faabric::util::setMockMode(false);
    }

  protected:
    faabric::transport::PointToPointBroker& broker;
};

class PointToPointClientServerFixture
  : public PointToPointTestFixture
  , SchedulerTestFixture
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

class TestExecutor final : public faabric::scheduler::Executor
{
  public:
    TestExecutor(faabric::Message& msg);

    faabric::util::MemoryRegion dummyMemory = nullptr;
    size_t dummyMemorySize = 0;

    void reset(faabric::Message& msg) override;

    void restore(faabric::Message& msg) override;

    faabric::util::MemoryView getMemoryView() override;

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

class SchedulingDecisionTestFixture : public SchedulerTestFixture
{
  public:
    SchedulingDecisionTestFixture()
    {
        faabric::util::setMockMode(true);

        std::shared_ptr<TestExecutorFactory> fac =
          std::make_shared<TestExecutorFactory>();
        setExecutorFactory(fac);
    }

    ~SchedulingDecisionTestFixture() { faabric::util::setMockMode(false); }

  protected:
    int appId = 123;
    int groupId = 456;
    std::string masterHost = faabric::util::getSystemConfig().endpointHost;

    // Helper struct to configure one scheduling decision
    struct SchedulingConfig
    {
        std::vector<std::string> hosts;
        std::vector<int> slots;
        int numReqs;
        faabric::util::SchedulingTopologyHint topologyHint;
        std::vector<std::string> expectedHosts;
    };

    // Helper method to set the available hosts and slots per host prior to
    // making a scheduling decision
    void setHostResources(std::vector<std::string> registeredHosts,
                          std::vector<int> slotsPerHost)
    {
        assert(registeredHosts.size() == slotsPerHost.size());
        auto& sch = faabric::scheduler::getScheduler();
        sch.clearRecordedMessages();

        for (int i = 0; i < registeredHosts.size(); i++) {
            faabric::HostResources resources;
            resources.set_slots(slotsPerHost.at(i));
            resources.set_usedslots(0);

            sch.addHostToGlobalSet(registeredHosts.at(i));

            // If setting resources for the master host, update the scheduler.
            // Otherwise, queue the resource response
            if (i == 0) {
                sch.setThisHostResources(resources);
            } else {
                faabric::scheduler::queueResourceResponse(registeredHosts.at(i),
                                                          resources);
            }
        }
    }

    void checkRecordedBatchMessages(
      faabric::util::SchedulingDecision actualDecision,
      const SchedulingConfig& config)
    {
        auto batchMessages = faabric::scheduler::getBatchRequests();

        // First, turn our expected list of hosts to a map with frequency count
        // and exclude the master host as no message is sent
        std::map<std::string, int> expectedHostCount;
        for (const auto& h : config.expectedHosts) {
            if (h != masterHost) {
                ++expectedHostCount[h];
            }
        }

        // Then check that the count matches the size of the batch sent
        for (const auto& hostReqPair : batchMessages) {
            REQUIRE(expectedHostCount.contains(hostReqPair.first));
            REQUIRE(expectedHostCount.at(hostReqPair.first) ==
                    hostReqPair.second->messages_size());
        }
    }

    // We test the scheduling decision twice: the first one will follow the
    // unregistered hosts path, the second one the registerd hosts one.
    void testActualSchedulingDecision(
      std::shared_ptr<faabric::BatchExecuteRequest> req,
      const SchedulingConfig& config)
    {
        faabric::util::SchedulingDecision actualDecision(appId, groupId);

        // Set resources for all hosts
        setHostResources(config.hosts, config.slots);

        // The first time we run the batch request, we will follow the
        // unregistered hosts path
        actualDecision = sch.callFunctions(req, config.topologyHint);
        REQUIRE(actualDecision.hosts == config.expectedHosts);
        checkRecordedBatchMessages(actualDecision, config);

        // We wait for the execution to finish and the scheduler to vacate
        // the slots. We can't wait on the function result, as sometimes
        // functions won't be executed at all (e.g. master running out of
        // resources).
        SLEEP_MS(100);

        // Set resources again to reset the used slots
        auto reqCopy =
          faabric::util::batchExecFactory("foo", "baz", req->messages_size());
        setHostResources(config.hosts, config.slots);

        // The second time we run the batch request, we will follow
        // the registered hosts path
        actualDecision = sch.callFunctions(reqCopy, config.topologyHint);
        REQUIRE(actualDecision.hosts == config.expectedHosts);
        checkRecordedBatchMessages(actualDecision, config);
    }
};
}
