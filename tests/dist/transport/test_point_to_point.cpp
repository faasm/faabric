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

    // Set up this host's resources
    // Make sure some functions execute remotely, some locally
    int nLocalSlots = 1;
    int nFuncs = 3;

    faabric::HostResources res;
    res.set_slots(nLocalSlots);
    sch.setThisHostResources(res);

    // Set up batch request
    std::shared_ptr<faabric::BatchExecuteRequest> req =
      faabric::util::batchExecFactory("ptp", "simple", nFuncs);

    // Double check app id
    int appId = req->messages().at(0).appid();
    REQUIRE(appId > 0);

    faabric::transport::PointToPointBroker& broker =
      faabric::transport::getPointToPointBroker();

    std::vector<std::string> expectedHosts = { getMasterIP(),
                                               getWorkerIP(),
                                               getWorkerIP() };

    // Set up individual messages
    // Note that this thread is acting as app index 0
    faabric::util::SchedulingDecision expectedDecision(appId);
    for (int i = 0; i < nFuncs; i++) {
        faabric::Message& msg = req->mutable_messages()->at(i);

        msg.set_appindex(i + 1);

        // Add to expected decision
        expectedDecision.addMessage(expectedHosts.at(i), req->messages().at(i));
    }

    // Call the functions
    faabric::util::SchedulingDecision actualDecision = sch.callFunctions(req);
    checkSchedulingDecisionEquality(actualDecision, expectedDecision);

    // Set up point-to-point mappings
    broker.setAndSendMappingsFromSchedulingDecision(actualDecision);

    // Check functions executed successfully
    for (int i = 0; i < nFuncs; i++) {
        faabric::Message& m = req->mutable_messages()->at(i);

        sch.getFunctionResult(m.id(), 2000);
        REQUIRE(m.returnvalue() == 0);
    }
}
}
