#include "faabric_utils.h"
#include <catch.hpp>

#include <faabric/scheduler/Scheduler.h>

namespace tests {
TEST_CASE("Test available hosts", "[scheduler][dist]")
{
    auto& sch = faabric::scheduler::getScheduler();

    std::set<std::string> actual = sch.getAvailableHosts();

    std::set<std::string> expected = {
        "master",
        "worker",
    };

    REQUIRE(actual == expected);
}
}
