#include <faabric/endpoint/FaabricEndpoint.h>
#include <faabric/planner/PlannerServer.h>
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
    faabric::planner::PlannerServer planner;
    // The RPC server starts in the background
    planner.start();

    // The faabric endpoint starts in the foreground
    SPDLOG_INFO("Starting planner endpoint");
    // TODO: implement custom HTTP endpoint specifically for the planner
    faabric::endpoint::FaabricEndpoint endpoint;
    endpoint.start(faabric::endpoint::EndpointMode::SIGNAL);

    SPDLOG_INFO("Planner server shutting down");
}
