#define CATCH_CONFIG_RUNNER

// Disable catch signal catching to avoid interfering with dirty tracking
#define CATCH_CONFIG_NO_POSIX_SIGNALS 1

#include <catch2/catch.hpp>

#include "DistTestExecutor.h"
#include "faabric_utils.h"
#include "init.h"

#include <faabric/runner/FaabricMain.h>
#include <faabric/scheduler/ExecutorFactory.h>
#include <faabric/transport/context.h>
#include <faabric/util/crash.h>
#include <faabric/util/logging.h>

FAABRIC_CATCH_LOGGER

int main(int argc, char* argv[])
{
    faabric::util::setUpCrashHandler();

    faabric::transport::initGlobalMessageContext();
    faabric::util::initLogging();
    tests::initDistTests();

    std::shared_ptr<faabric::scheduler::ExecutorFactory> fac =
      std::make_shared<tests::DistTestExecutorFactory>();

    // WARNING: all 0MQ sockets have to have gone *out of scope* before we shut
    // down the context, therefore this segment must be in a nested scope (or
    // another function).
    int result;
    {
        SPDLOG_INFO("Starting distributed test server on master");
        faabric::runner::FaabricMain m(fac);
        m.startBackground();

        // Run the tests
        result = Catch::Session().run(argc, argv);
        fflush(stdout);

        // Shut down
        SPDLOG_INFO("Shutting down");
        m.shutdown();
    }

    faabric::transport::closeGlobalMessageContext();

    return result;
}
