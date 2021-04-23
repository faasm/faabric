#include "faabric_utils.h"
#include <catch.hpp>

#include <faabric/executor/DummyExecutor.h>

using namespace faabric::executor;

namespace tests {

TEST_CASE("Test executing simple function", "[executor]")
{
    cleanFaabric();

    DummyExecutor d(0);
}
}
