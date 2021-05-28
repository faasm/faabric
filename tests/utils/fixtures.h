#pragma once

#include "faabric/snapshot/SnapshotRegistry.h"
#include "faabric/util/memory.h"
#include <faabric/redis/Redis.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/testing.h>

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
}
