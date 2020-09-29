#define CATCH_CONFIG_RUNNER

#include "faabric_utils.h"
#include <catch/catch.hpp>
#include <faabric/util/logging.h>

int main(int argc, char* argv[])
{
    faabric::util::initLogging();

    int result = Catch::Session().run(argc, argv);

    fflush(stdout);

    return result;
}