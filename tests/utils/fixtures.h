#pragma once

#include <faabric/redis/Redis.h>
#include <faabric/scheduler/ExecutorFactory.h>
#include <faabric/scheduler/MpiWorld.h>
#include <faabric/scheduler/MpiWorldRegistry.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/state/State.h>
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
        faabric::scheduler::clearMockSnapshotRequests();

        sch.shutdown();
        sch.addHostToGlobalSet();
    };

    ~SchedulerTestFixture()
    {
        faabric::util::setMockMode(false);
        faabric::util::setTestMode(true);

        faabric::scheduler::clearMockRequests();
        faabric::scheduler::clearMockSnapshotRequests();

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
        reg.clear();
    }

    ~SnapshotTestFixture() { reg.clear(); }

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

class MessageContextFixture : public SchedulerTestFixture
{
  public:
    MessageContextFixture() {}

    ~MessageContextFixture() {}
};

class MpiBaseTestFixture : public SchedulerTestFixture
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

class RemoteMpiTestFixture : public MpiBaseTestFixture
{
  public:
    RemoteMpiTestFixture()
      : thisHost(faabric::util::getSystemConfig().endpointHost)
      , otherHost(LOCALHOST)
    {
        remoteWorld.overrideHost(otherHost);
    }

    void setWorldsSizes(int worldSize, int ranksWorldOne, int ranksWorldTwo)
    {
        // Update message
        msg.set_mpiworldsize(worldSize);

        // Set local ranks
        faabric::HostResources localResources;
        localResources.set_slots(ranksWorldOne);
        // Account for the master rank that is already running in this world
        localResources.set_usedslots(1);
        // Set remote ranks
        faabric::HostResources otherResources;
        otherResources.set_slots(ranksWorldTwo);
        // Note that the remaining ranks will be allocated to the world
        // with the master host

        std::string otherHost = LOCALHOST;
        sch.addHostToGlobalSet(otherHost);

        // Mock everything to make sure the other host has resources as well
        faabric::util::setMockMode(true);
        sch.setThisHostResources(localResources);
        faabric::scheduler::queueResourceResponse(otherHost, otherResources);
    }

  protected:
    std::string thisHost;
    std::string otherHost;

    faabric::scheduler::MpiWorld remoteWorld;
};
}
