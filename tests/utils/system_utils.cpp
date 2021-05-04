#include "DummyExecutorFactory.h"
#include <catch.hpp>

#include <faabric/proto/faabric.pb.h>
#include <faabric/redis/Redis.h>
#include <faabric/scheduler/ExecutorFactory.h>
#include <faabric/scheduler/FunctionCallClient.h>
#include <faabric/scheduler/MpiWorldRegistry.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/scheduler/SnapshotClient.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/state/State.h>
#include <faabric/util/testing.h>
#include <faabric_utils.h>

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

    // Give scheduler enough resources
    faabric::HostResources res;
    res.set_slots(10);
    sch.setThisHostResources(res);

    // Clear snapshots
    faabric::snapshot::getSnapshotRegistry().clear();

    // Reset system config
    conf.reset();

    // Set test mode back on and mock mode off
    faabric::util::setTestMode(true);
    faabric::util::setMockMode(false);
    faabric::scheduler::clearMockRequests();
    faabric::scheduler::clearMockSnapshotRequests();

    // Set up dummy executor factory
    std::shared_ptr<faabric::scheduler::ExecutorFactory> fac =
      std::make_shared<faabric::scheduler::DummyExecutorFactory>();
    faabric::scheduler::setExecutorFactory(fac);

    // Clear out MPI worlds
    scheduler::MpiWorldRegistry& mpiRegistry = scheduler::getMpiWorldRegistry();
    mpiRegistry.clear();
}
}
