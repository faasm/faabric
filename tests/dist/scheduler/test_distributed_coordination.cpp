#include <catch.hpp>

#include "faabric_utils.h"
#include "fixtures.h"
#include "init.h"

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/DistributedCoordinator.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/logging.h>

namespace tests {

TEST_CASE_METHOD(DistTestsFixture,
                 "Test distributed barrier coordination",
                 "[sync]")
{
    // Set up this host's resources, force execution across hosts
    int nChainedFuncs = 4;
    int nLocalSlots = 2;

    faabric::HostResources res;
    res.set_slots(nLocalSlots);
    sch.setThisHostResources(res);

    // Set up the message
    std::shared_ptr<faabric::BatchExecuteRequest> req =
      faabric::util::batchExecFactory("coord", "barrier", 1);

    // Set number of chained funcs
    faabric::Message& m = req->mutable_messages()->at(0);
    m.set_inputdata(std::to_string(nChainedFuncs));

    // Call the function
    std::vector<std::string> expectedHosts = { getMasterIP() };
    std::vector<std::string> executedHosts = sch.callFunctions(req);
    REQUIRE(expectedHosts == executedHosts);

    // Get result
    faabric::Message result = sch.getFunctionResult(m.id(), 10000);
    REQUIRE(result.returnvalue() == 0);

    // Check state written by all chained funcs
}
}
