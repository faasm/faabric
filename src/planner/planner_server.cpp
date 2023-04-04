#include <faabric/planner/PlannerServer.h>
#include <faabric/util/crash.h>
#include <faabric/util/logging.h>

int main()
{
    // Initialise logging
    faabric::util::initLogging();

    // Initialise crash handler
    faabric::util::setUpCrashHandler();

    // Start planner server in the foreground
    SPDLOG_INFO("Starting planner server");
    faabric::planner::PlannerServer planner;
    planner.start();

    SPDLOG_INFO("Planner server shutting down");
}
