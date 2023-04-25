#include <catch2/catch.hpp>

#include "dist_test_fixtures.h"
#include "faabric_utils.h"
#include "init.h"

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/config.h>
#include <faabric/util/func.h>
#include <faabric/util/logging.h>
#include <faabric/util/scheduling.h>

namespace tests {

TEST_CASE_METHOD(DistTestsFixture,
                 "Test executing functions on multiple hosts",
                 "[funcs]")
{
    // Set up this host's resources
    int nLocalSlots = 6;
    int nFuncs = 8;
    faabric::HostResources res;
    res.set_slots(nLocalSlots);
    sch.setThisHostResources(res);

    std::string thisHost = conf.endpointHost;
    std::string otherHost = getWorkerIP();

    // Set up the messages
    std::shared_ptr<faabric::BatchExecuteRequest> req =
      faabric::util::batchExecFactory("funcs", "simple", nFuncs);

    // Set up the expectation
    const faabric::Message firstMsg = req->messages().at(0);
    faabric::util::SchedulingDecision expectedDecision(firstMsg.appid(),
                                                       firstMsg.groupid());
    for (int i = 0; i < req->messages_size(); i++) {
        std::string host = i < nLocalSlots ? thisHost : otherHost;
        expectedDecision.addMessage(host, req->messages().at(i));
    }

    // Call the functions
    faabric::util::SchedulingDecision actualDecision = sch.callFunctions(req);

    // Check decision is as expected
    checkSchedulingDecisionEquality(actualDecision, expectedDecision);

    // Check functions executed on this host
    for (int i = 0; i < req->messages_size(); i++) {
        std::string host = i < nLocalSlots ? thisHost : otherHost;
        auto m = req->messages(i);
        auto res = sch.getFunctionResult(m, 1000);
        std::string expected =
          fmt::format("Function {} executed on host {}", m.id(), host);

        REQUIRE(res.outputdata() == expected);
    }
}
}
