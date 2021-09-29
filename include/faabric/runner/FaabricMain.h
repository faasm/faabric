#pragma once

#include <faabric/scheduler/ExecutorFactory.h>
#include <faabric/scheduler/FunctionCallServer.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/snapshot/SnapshotServer.h>
#include <faabric/state/StateServer.h>
#include <faabric/util/config.h>

namespace faabric::runner {
class FaabricMain
{
  public:
    FaabricMain(std::shared_ptr<faabric::scheduler::ExecutorFactory> fac);

    void startBackground();

    void startRunner();

    void startFunctionCallServer();

    void startStateServer();

    void startSnapshotServer();

    void shutdown();

  private:
    void setupCrashHandler();

    faabric::state::StateServer stateServer;
    faabric::scheduler::FunctionCallServer functionServer;
    faabric::snapshot::SnapshotServer snapshotServer;
};
}
