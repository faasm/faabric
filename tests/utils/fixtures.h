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

    uint8_t* allocatePages(int nPages)
    {
        return (uint8_t*)mmap(nullptr,
                              nPages * faabric::util::HOST_PAGE_SIZE,
                              PROT_WRITE,
                              MAP_SHARED | MAP_ANONYMOUS,
                              -1,
                              0);
    }

    void deallocatePages(uint8_t* base, int nPages)
    {
        munmap(base, nPages * faabric::util::HOST_PAGE_SIZE);
    }

    faabric::util::SnapshotData takeSnapshot(const std::string& snapKey,
                                             int nPages,
                                             bool locallyRestorable)
    {
        faabric::util::SnapshotData snap;
        uint8_t* data = allocatePages(nPages);

        snap.size = nPages * faabric::util::HOST_PAGE_SIZE;
        snap.data = data;

        reg.takeSnapshot(snapKey, snap, locallyRestorable);

        return snap;
    }

    void removeSnapshot(const std::string& key, int nPages)
    {
        faabric::util::SnapshotData snap = reg.getSnapshot(key);
        deallocatePages(snap.data, nPages);
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
}
