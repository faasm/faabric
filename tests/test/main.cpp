#define CATCH_CONFIG_RUNNER

#include <catch.hpp>

#include "faabric_utils.h"
#include <faabric/util/logging.h>

int main(int argc, char* argv[])
{
    int result = Catch::Session().run(argc, argv);

    fflush(stdout);

    return result;
}
