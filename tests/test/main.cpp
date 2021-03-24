#define CATCH_CONFIG_RUNNER

#include <catch.hpp>

#include "faabric_utils.h"
#include <faabric/util/logging.h>
#include <faabric/util/testing.h>

int main(int argc, char* argv[])
{
    int result = Catch::Session().run(argc, argv);

    faabric::util::setTestMode(true);

    fflush(stdout);

    return result;
}
