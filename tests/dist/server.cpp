#include "DistTestExecutor.h"
#include "init.h"

#include <faabric/endpoint/FaabricEndpoint.h>
#include <faabric/executor/ExecutorFactory.h>
#include <faabric/runner/FaabricMain.h>
#include <faabric/util/logging.h>

int main()
{
    faabric::util::initLogging();

    tests::initDistTests();

    int slots = 4;
    SPDLOG_INFO("Forcing distributed test server to have {} slots", slots);
    faabric::HostResources res;
    res.set_slots(slots);
    faabric::scheduler::getScheduler().setThisHostResources(res);

    SPDLOG_INFO("Starting distributed test server on worker");
    std::shared_ptr<faabric::executor::ExecutorFactory> fac =
      std::make_shared<tests::DistTestExecutorFactory>();
    faabric::runner::FaabricMain faabricMain(fac);
    faabricMain.startBackground();

    SPDLOG_INFO("---------------------------------");
    SPDLOG_INFO("Distributed test server started");
    SPDLOG_INFO("---------------------------------");

    // Endpoint will block until killed
    SPDLOG_INFO("Starting HTTP endpoint on worker");
    faabric::endpoint::FaabricEndpoint endpoint;
    endpoint.start(faabric::endpoint::EndpointMode::SIGNAL);

    SPDLOG_INFO("Shutting down");
    faabricMain.shutdown();

    return EXIT_SUCCESS;
}
