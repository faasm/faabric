#pragma once

#include "faabric/scheduler/SnapshotServer.h"
#include <faabric/executor/FaabricPool.h>
#include <faabric/state/StateServer.h>
#include <faabric/util/config.h>

namespace faabric::executor {
class FaabricMain
{
  public:
    void startBackground();

    void startFunctionCallServer();

    void startStateServer();

    void startSnapshotServer();

    void shutdown();
  private:
    faabric::state::StateServer stateServer;
    faabric::scheduler::FunctionCallServer functionServer;
    faabric::scheduler::SnapshotServer snapshotServer;
};
}
