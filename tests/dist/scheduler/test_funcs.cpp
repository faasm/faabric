#include <catch2/catch.hpp>

#include "dist_test_fixtures.h"
#include "faabric_utils.h"
#include "init.h"

#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/config.h>
#include <faabric/util/func.h>
#include <faabric/util/logging.h>

namespace tests {

TEST_CASE_METHOD(DistTestsFixture,
                 "Test executing functions on multiple hosts",
                 "[funcs]")
{
    std::string thisHost = conf.endpointHost;
    std::string otherHost = getWorkerIP();

    // Set up this host's resources (2 functions locally, 2 remotely)
    int nLocalSlots = 2;
    int nFuncs = 4;
    faabric::HostResources res;
    res.set_slots(nFuncs);
    res.set_usedslots(nLocalSlots);
    sch.setThisHostResources(res);
    res.set_slots(nLocalSlots);
    res.set_usedslots(0);
    sch.addHostToGlobalSet(otherHost, std::make_shared<HostResources>(res));

    // Set up the messages
    std::shared_ptr<faabric::BatchExecuteRequest> req =
      faabric::util::batchExecFactory("funcs", "simple", nFuncs);

    // Set up the expectation
    const faabric::Message firstMsg = req->messages().at(0);
    faabric::batch_scheduler::SchedulingDecision expectedDecision(
      firstMsg.appid(), firstMsg.groupid());
    expectedDecision.addMessage(thisHost, req->messages().at(0));
    expectedDecision.addMessage(thisHost, req->messages().at(1));
    expectedDecision.addMessage(otherHost, req->messages().at(2));
    expectedDecision.addMessage(otherHost, req->messages().at(3));

    // Call the functions
    auto actualDecision = plannerCli.callFunctions(req);

    // Check decision is as expected
    checkSchedulingDecisionEquality(actualDecision, expectedDecision);

    // Check functions executed on this host
    for (int i = 0; i < nLocalSlots; i++) {
        faabric::Message& m = req->mutable_messages()->at(i);

        auto resultMsg = plannerCli.getMessageResult(m, 1000);
        std::string expected =
          fmt::format("Function {} executed on host {}", m.id(), getMasterIP());

        REQUIRE(resultMsg.outputdata() == expected);
    }

    // Check functions executed on the other host
    for (int i = nLocalSlots; i < nFuncs; i++) {
        faabric::Message& m = req->mutable_messages()->at(i);
        faabric::Message result = plannerCli.getMessageResult(m, 1000);

        std::string expected =
          fmt::format("Function {} executed on host {}", m.id(), getWorkerIP());

        REQUIRE(result.outputdata() == expected);
    }
}
}
