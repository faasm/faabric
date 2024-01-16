#include <faabric/executor/ExecutorFactory.h>
#include <faabric/planner/PlannerClient.h>
#include <faabric/runner/FaabricMain.h>
#include <faabric/scheduler/FunctionCallServer.h>
#include <faabric/util/config.h>
#include <faabric/util/crash.h>
#include <faabric/util/logging.h>
#include <faabric/util/timing.h>

namespace faabric::runner {
FaabricMain::FaabricMain(
  std::shared_ptr<faabric::executor::ExecutorFactory> execFactory)
  : stateServer(faabric::state::getGlobalState())
{
    faabric::executor::setExecutorFactory(execFactory);
}

void FaabricMain::startBackground()
{
    // Crash handler
    faabric::util::setUpCrashHandler();

    PROF_BEGIN

    // Start basics
    startRunner();

    // In-memory state
    startStateServer();

    // Snapshots
    startSnapshotServer();

    // Point-to-point messaging
    startPointToPointServer();

    // Work sharing
    startFunctionCallServer();

    PROF_SUMMARY
}

void FaabricMain::startRunner()
{
    // Ensure we can ping both redis instances
    faabric::redis::Redis::getQueue().ping();
    faabric::redis::Redis::getState().ping();

    // Ensure we can ping the planner
    faabric::planner::getPlannerClient().ping();

    auto& sch = faabric::scheduler::getScheduler();
    sch.addHostToGlobalSet();
}

void FaabricMain::startFunctionCallServer()
{
    SPDLOG_INFO("Starting function call server");
    functionServer.start();
}

void FaabricMain::startSnapshotServer()
{
    SPDLOG_INFO("Starting snapshot server");
    snapshotServer.start();
}

void FaabricMain::startPointToPointServer()
{
    SPDLOG_INFO("Starting point-to-point server");
    pointToPointServer.start();
}

void FaabricMain::startStateServer()
{
    // Skip state server if not in inmemory mode
    faabric::util::SystemConfig& conf = faabric::util::getSystemConfig();
    if (conf.stateMode != "inmemory") {
        SPDLOG_INFO("Not starting state server in state mode {}",
                    conf.stateMode);
        return;
    }

    // Note that the state server spawns its own background thread
    SPDLOG_INFO("Starting state server");
    stateServer.start();
}

void FaabricMain::shutdown()
{
    SPDLOG_INFO("Removing from global working set");

    SPDLOG_INFO("Waiting for the state server to finish");
    stateServer.stop();

    SPDLOG_INFO("Waiting for the function server to finish");
    functionServer.stop();

    SPDLOG_INFO("Waiting for the snapshot server to finish");
    snapshotServer.stop();

    SPDLOG_INFO("Waiting for the point-to-point server to finish");
    pointToPointServer.stop();

    auto& sch = faabric::scheduler::getScheduler();
    sch.shutdown();

    SPDLOG_INFO("Faabric pool successfully shut down");
}
}
