#include "faabric_utils.h"
#include <catch.hpp>

#include <faabric/scheduler/Scheduler.h>

namespace tests {
TEST_CASE("Test available hosts", "[scheduler][dist]")
{
    auto& sch = faabric::scheduler::getScheduler();
    sch.addHostToGlobalSet();

    std::set<std::string> actual = sch.getAvailableHosts();

    REQUIRE(actual.size() == 2);
}
}
