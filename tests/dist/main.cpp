#define CATCH_CONFIG_RUNNER

#include "DistTestExecutor.h"
#include "faabric_utils.h"
#include <catch.hpp>

#include "init.h"

#include <faabric/endpoint/FaabricEndpoint.h>
#include <faabric/runner/FaabricMain.h>
#include <faabric/scheduler/ExecutorFactory.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>

using namespace faabric::scheduler;

FAABRIC_CATCH_LOGGER

int main(int argc, char* argv[])
{
    faabric::util::initLogging();

    // Set up the distributed tests
    tests::initDistTests();

    // Start everything up
    SPDLOG_INFO("Starting distributed test server on master");
    std::shared_ptr<ExecutorFactory> fac =
      std::make_shared<tests::DistTestExecutorFactory>();
    faabric::runner::FaabricMain m(fac);
    m.startBackground();

    // Wait for things to start
    SLEEP_MS(3000);

    // Run the tests
    int result = Catch::Session().run(argc, argv);
    fflush(stdout);

    // Shut down
    SPDLOG_INFO("Shutting down");
    m.shutdown();

    return result;
}
