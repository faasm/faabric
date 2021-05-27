#pragma once

#include "faabric/util/memory.h"
#include <faabric/redis/Redis.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/testing.h>

namespace tests {
class BaseTestFixture
{
  public:
    BaseTestFixture()
      : sch(faabric::scheduler::getScheduler())
      , conf(faabric::util::getSystemConfig())
      , redis(faabric::redis::Redis::getQueue())
    {
        faabric::util::resetDirtyTracking();

        faabric::util::setMockMode(false);
        faabric::util::setTestMode(true);

        faabric::scheduler::clearMockRequests();
        faabric::scheduler::clearMockSnapshotRequests();

        redis.flushAll();

        sch.shutdown();
        sch.addHostToGlobalSet();
    };

    ~BaseTestFixture()
    {
        faabric::util::setMockMode(false);
        faabric::util::setTestMode(true);

        faabric::scheduler::clearMockRequests();
        faabric::scheduler::clearMockSnapshotRequests();

        sch.shutdown();
        sch.addHostToGlobalSet();

        conf.reset();

        redis.flushAll();

        faabric::util::resetDirtyTracking();
    };

  protected:
    faabric::scheduler::Scheduler& sch;
    faabric::util::SystemConfig& conf;
    faabric::redis::Redis& redis;
};
}
