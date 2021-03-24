#include <catch.hpp>

#include "faabric/util/testing.h"
#include "faabric_utils.h"

#include <faabric/redis/Redis.h>
#include <faabric/scheduler/FunctionCallClient.h>
#include <faabric/scheduler/MpiWorldRegistry.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/state/State.h>

namespace tests {
void cleanFaabric()
{
    faabric::util::SystemConfig& conf = faabric::util::getSystemConfig();

    // Clear out Redis
    redis::Redis::getState().flushAll();
    redis::Redis::getQueue().flushAll();

    // Clear out any cached state, do so for both modes
    std::string& originalStateMode = conf.stateMode;
    conf.stateMode = "inmemory";
    state::getGlobalState().forceClearAll(true);
    conf.stateMode = "redis";
    state::getGlobalState().forceClearAll(true);
    conf.stateMode = originalStateMode;

    // Reset scheduler
    scheduler::Scheduler& sch = scheduler::getScheduler();
    sch.shutdown();
    sch.addHostToGlobalSet();

    // Reset system config
    conf.reset();

    // Set test mode back on and mock mode off
    faabric::util::setTestMode(true);
    faabric::util::setMockMode(false);

    // Clear out MPI worlds
    scheduler::MpiWorldRegistry& mpiRegistry = scheduler::getMpiWorldRegistry();
    mpiRegistry.clear();
}
}
