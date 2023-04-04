#include <catch2/catch.hpp>

#include <faabric/planner/PlannerClient.h>

using namespace faabric::planner;

namespace tests {
TEST_CASE("Test basic planner client operations", "[planner]")
{
    PlannerClient cli;

    REQUIRE_NOTHROWS(cli.ping());
}
}
