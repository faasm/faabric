#define CATCH_CONFIG_RUNNER

#include "DistTestExecutor.h"
#include "faabric_utils.h"
#include <catch.hpp>

#include "init.h"

#include <faabric/endpoint/FaabricEndpoint.h>
#include <faabric/runner/FaabricMain.h>
#include <faabric/scheduler/ExecutorFactory.h>
#include <faabric/util/logging.h>

using namespace faabric::scheduler;

struct LogListener : Catch::TestEventListenerBase
{
    using TestEventListenerBase::TestEventListenerBase;

    void testCaseStarting(Catch::TestCaseInfo const& testInfo) override
    {
        auto logger = faabric::util::getLogger();
        logger->info("---------------------------------------------");
        logger->info("TEST: {}", testInfo.name);
        logger->info("---------------------------------------------");
    }
};

CATCH_REGISTER_LISTENER(LogListener)

int main(int argc, char* argv[])
{
    const auto& logger = faabric::util::getLogger();

    // Set up the distributed tests
    tests::initDistTests();

    // Start everything up
    logger->info("Starting executor pool in the background");
    std::shared_ptr<ExecutorFactory> fac =
      std::make_shared<tests::DistTestExecutorFactory>();
    faabric::runner::FaabricMain m(fac);
    m.startBackground();

    // Wait for things to start
    usleep(3000 * 1000);

    // Run the tests
    int result = Catch::Session().run(argc, argv);
    fflush(stdout);

    // Shut down
    logger->info("Shutting down");
    m.shutdown();

    return result;
}
