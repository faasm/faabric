#define CATCH_CONFIG_RUNNER

#include <catch.hpp>

#include "faabric_utils.h"

#include <faabric/transport/MessageContext.h>
#include <faabric/util/logging.h>
#include <faabric/util/testing.h>

FAABRIC_CATCH_LOGGER

int main(int argc, char* argv[])
{
    faabric::util::setTestMode(true);
    faabric::util::initLogging();

    int result = Catch::Session().run(argc, argv);

    fflush(stdout);

    faabric::transport::getGlobalMessageContext()->shutdown();
    faabric::transport::getGlobalMessageContext()->close();

    return result;
}
