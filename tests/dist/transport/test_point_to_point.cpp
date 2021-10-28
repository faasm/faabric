#include <catch2/catch.hpp>

#include "faabric_utils.h"
#include "fixtures.h"
#include "init.h"

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/config.h>
#include <faabric/util/func.h>
#include <faabric/util/logging.h>
#include <faabric/util/scheduling.h>

namespace tests {

TEST_CASE_METHOD(DistTestsFixture,
                 "Test point-to-point messaging on multiple hosts",
                 "[ptp]")
{
    std::set<std::string> actualAvailable = sch.getAvailableHosts();
    std::set<std::string> expectedAvailable = { getMasterIP(), getWorkerIP() };
    REQUIRE(actualAvailable == expectedAvailable);

    int appId = 222;
    int groupId = 333;

    // Set up this host's resources
    // Make sure some functions execute remotely, some locally
    int nLocalSlots = 1;
    int nFuncs = 4;

    faabric::HostResources res;
    res.set_slots(nLocalSlots);
    sch.setThisHostResources(res);

    // Set up batch request
    std::shared_ptr<faabric::BatchExecuteRequest> req =
      faabric::util::batchExecFactory("ptp", "simple", nFuncs);

    // Prepare expected decision
    faabric::util::SchedulingDecision expectedDecision(appId, groupId);
    std::vector<std::string> expectedHosts = {
        getMasterIP(), getWorkerIP(), getWorkerIP(), getWorkerIP()
    };

    // Set up individual messages
    for (int i = 0; i < nFuncs; i++) {
        faabric::Message& msg = req->mutable_messages()->at(i);

        msg.set_appid(appId);
        msg.set_appidx(i);
        msg.set_groupid(groupId);
        msg.set_groupidx(i);

        // Add to expected decision
        expectedDecision.addMessage(expectedHosts.at(i), req->messages().at(i));
    }

    // Call the functions
    faabric::util::SchedulingDecision actualDecision = sch.callFunctions(req);
    checkSchedulingDecisionEquality(actualDecision, expectedDecision);

    // Check functions executed successfully
    for (int i = 0; i < nFuncs; i++) {
        faabric::Message& m = req->mutable_messages()->at(i);

        sch.getFunctionResult(m.id(), 2000);
        REQUIRE(m.returnvalue() == 0);
    }
}

TEST_CASE_METHOD(DistTestsFixture,
                 "Test distributed barrier coordination",
                 "[ptp]")
{
    // Set up this host's resources, force execution across hosts
    int nChainedFuncs = 4;
    int nLocalSlots = 2;

    faabric::HostResources res;
    res.set_slots(nLocalSlots);
    sch.setThisHostResources(res);

    // Set up the message
    std::shared_ptr<faabric::BatchExecuteRequest> req =
      faabric::util::batchExecFactory("ptp", "barrier", 1);

    // Set number of chained funcs
    faabric::Message& m = req->mutable_messages()->at(0);
    m.set_inputdata(std::to_string(nChainedFuncs));

    // Call the function
    std::vector<std::string> expectedHosts = { getMasterIP() };
    std::vector<std::string> executedHosts = sch.callFunctions(req).hosts;
    REQUIRE(expectedHosts == executedHosts);

    // Get result
    faabric::Message result = sch.getFunctionResult(m.id(), 10000);
    REQUIRE(result.returnvalue() == 0);
}
}
