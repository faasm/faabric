#include "faabric_utils.h"
#include <catch.hpp>

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
                 "Test executing functions on multiple hosts",
                 "[funcs]")
{
    // Set up this host's resources
    int nLocalSlots = 2;
    int nFuncs = 4;
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
    expectedDecision.addMessage(thisHost, req->messages().at(0));
    expectedDecision.addMessage(thisHost, req->messages().at(1));
    expectedDecision.addMessage(otherHost, req->messages().at(2));
    expectedDecision.addMessage(otherHost, req->messages().at(3));

    // Call the functions
    faabric::util::SchedulingDecision actualDecision = sch.callFunctions(req);

    // Check decision is as expected
    checkSchedulingDecisionEquality(actualDecision, expectedDecision);

    // Check functions executed on this host
    for (int i = 0; i < nLocalSlots; i++) {
        faabric::Message& m = req->mutable_messages()->at(i);

        sch.getFunctionResult(m.id(), 1000);
        std::string expected =
          fmt::format("Function {} executed on host {}", m.id(), getMasterIP());

        REQUIRE(m.outputdata() == expected);
    }

    // Check functions executed on the other host
    for (int i = nLocalSlots; i < nFuncs; i++) {
        faabric::Message& m = req->mutable_messages()->at(i);
        faabric::Message result = sch.getFunctionResult(m.id(), 1000);

        std::string expected =
          fmt::format("Function {} executed on host {}", m.id(), getWorkerIP());

        REQUIRE(result.outputdata() == expected);
    }
}
}
