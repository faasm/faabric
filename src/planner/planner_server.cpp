#include <faabric/endpoint/FaabricEndpoint.h>
#include <faabric/planner/PlannerEndpointHandler.h>
#include <faabric/planner/PlannerServer.h>
#include <faabric/snapshot/SnapshotServer.h>
#include <faabric/util/config.h>
#include <faabric/util/crash.h>
#include <faabric/util/logging.h>

int main()
{
    // Initialise logging
    faabric::util::initLogging();

    // Initialise crash handler
    faabric::util::setUpCrashHandler();

    // Start both the planner server and the planner http endpoint
    SPDLOG_INFO("Starting planner server");
    faabric::planner::PlannerServer plannerServer;
    // The RPC server starts in the background
    plannerServer.start();

    // Start also a snapshot server to synchronise snapshots
    SPDLOG_INFO("Starting planner snapshot server");
    faabric::snapshot::SnapshotServer snapshotServer;
    snapshotServer.start();

    // The faabric endpoint starts in the foreground
    SPDLOG_INFO("Starting planner endpoint");
    // We get the port from the global config, but the number of threads from
    // the planner config
    faabric::endpoint::FaabricEndpoint endpoint(
      faabric::util::getSystemConfig().plannerPort,
      faabric::planner::getPlanner().getConfig().numthreadshttpserver(),
      std::make_shared<faabric::planner::PlannerEndpointHandler>());
    endpoint.start(faabric::endpoint::EndpointMode::SIGNAL);

    SPDLOG_INFO("Planner snapshot server shutting down");
    snapshotServer.stop();

    SPDLOG_INFO("Planner server shutting down");
    plannerServer.stop();
}
