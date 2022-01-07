#define CATCH_CONFIG_RUNNER

#include <catch2/catch.hpp>

#include "faabric_utils.h"

#include <faabric/transport/context.h>
#include <faabric/util/crash.h>
#include <faabric/util/logging.h>
#include <faabric/util/testing.h>

#define CATCH_CONFIG_NO_POSIX_SIGNALS

FAABRIC_CATCH_LOGGER

int main(int argc, char* argv[])
{
    faabric::util::setUpCrashHandler();

    faabric::transport::initGlobalMessageContext();
    faabric::util::setTestMode(true);
    faabric::util::initLogging();

    int result = Catch::Session().run(argc, argv);

    fflush(stdout);

    faabric::transport::closeGlobalMessageContext();

    return result;
}
