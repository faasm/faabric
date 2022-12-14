#define CATCH_CONFIG_RUNNER

// Disable catch signal catching to avoid interfering with dirty tracking
#define CATCH_CONFIG_NO_POSIX_SIGNALS 1

#include <catch2/catch.hpp>

#include "faabric_utils.h"

#include <faabric/util/crash.h>
#include <faabric/util/logging.h>
#include <faabric/util/testing.h>

FAABRIC_CATCH_LOGGER

int main(int argc, char* argv[])
{
    faabric::util::setUpCrashHandler();

    faabric::util::setTestMode(true);
    faabric::util::initLogging();

    int result = Catch::Session().run(argc, argv);

    fflush(stdout);
    return result;
}
