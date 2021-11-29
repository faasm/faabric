#include "faabric_utils.h"
#include <catch2/catch.hpp>

#include "fixtures.h"
#include "init.h"

#include <faabric/scheduler/Scheduler.h>

namespace tests {

TEST_CASE_METHOD(MpiDistTestsFixture,
                 "Test geting execution graph from distributed execution",
                 "[mpi]")
{
    // Will allocate 2 ranks locally, and two ranks remotely
    int worldSize = 4;
    int nLocalSlots = 2;
    setLocalSlots(nLocalSlots);

    // By default, setRequest already sets the flag to record the exec graph
    auto req = setRequest("bcast");

    // Call functions
    sch.callFunctions(req);

    // Get function result
    faabric::Message& msg = req->mutable_messages()->at(0);
    faabric::Message result = sch.getFunctionResult(msg.id(), 1000);
    REQUIRE(result.returnvalue() == 0);

    // Get execution graph
    // TODO - remove this sleep when #181 is merged and rebased
    SLEEP_MS(500);
    auto execGraph = sch.getFunctionExecGraph(msg.id());

    // Do checks on the execution graph
    std::vector<std::string> expectedHosts = {
        getMasterIP(), getMasterIP(), getWorkerIP(), getWorkerIP()
    };
    std::set<std::string> expectedHostSet =
      std::set<std::string>(expectedHosts.begin(), expectedHosts.end());
    REQUIRE(worldSize == faabric::scheduler::countExecGraphNodes(execGraph));
    REQUIRE(expectedHostSet ==
            faabric::scheduler::getExecGraphHosts(execGraph));
    REQUIRE(expectedHosts ==
            faabric::scheduler::getMpiRankHostsFromExecGraph(execGraph));
}
}
