#pragma once

#include <sys/mman.h>

#include <faabric/proto/faabric.pb.h>
#include <faabric/redis/Redis.h>
#include <faabric/scheduler/ExecutorContext.h>
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
#include <faabric/util/dirty.h>
#include <faabric/util/func.h>
#include <faabric/util/latch.h>
#include <faabric/util/memory.h>
#include <faabric/util/network.h>
#include <faabric/util/scheduling.h>
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

class SchedulerTestFixture : public CachedDecisionTestFixture
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

        faabric::util::getDirtyTracker()->clearAll();
    };

    // Helper method to set the available hosts and slots per host prior to
    // making a scheduling decision
    void setHostResources(std::vector<std::string> hosts,
                          std::vector<int> slotsPerHost)
    {
        assert(hosts.size() == slotsPerHost.size());
        sch.clearRecordedMessages();
        faabric::scheduler::clearMockRequests();

        for (int i = 0; i < hosts.size(); i++) {
            faabric::HostResources resources;
            resources.set_slots(slotsPerHost.at(i));
            resources.set_usedslots(0);

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

    void reset(faabric::Message& msg) override;

    void restore(const std::string& snapshotKey) override;

    std::span<uint8_t> getMemoryView() override;

    void setUpDummyMemory(size_t memSize);

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
