#include "faabric/util/scheduling.h"
#include "faabric_utils.h"
#include <catch.hpp>

#include "fixtures.h"
#include "init.h"

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/config.h>
#include <faabric/util/func.h>
#include <faabric/util/logging.h>

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

    faabric::util::SchedulingDecision expectedDecision(appId);
    expectedDecision.addMessage(getMasterIP(), req->messages().at(0));
    expectedDecision.addMessage(getWorkerIP(), req->messages().at(1));
    expectedDecision.addMessage(getWorkerIP(), req->messages().at(2));

    // Set up individual messages
    // Note that this thread is acting as app index 0
    for (int i = 0; i < nFuncs; i++) {
        faabric::Message& msg = req->mutable_messages()->at(i);

        msg.set_appindex(i + 1);

        // Register function locations to where we assume they'll be executed
        // (we'll confirm this is the case after scheduling)
        broker.setHostForReceiver(
          msg.appid(), msg.appindex(), expectedHosts.at(i));
    }

    // Call the functions
    faabric::util::SchedulingDecision actualDecision = sch.callFunctions(req);
    checkSchedulingDecisionEquality(actualDecision, expectedDecision);

    // Broadcast mappings to other hosts
    broker.broadcastMappings(appId);

    // Send kick-off message to all functions
    std::vector<uint8_t> kickOffData = { 0, 1, 2 };
    for (int i = 0; i < nFuncs; i++) {
        broker.sendMessage(
          appId, 0, i + 1, kickOffData.data(), kickOffData.size());
    }

    // Check other functions executed successfully
    for (int i = 0; i < nFuncs; i++) {
        faabric::Message& m = req->mutable_messages()->at(i);

        sch.getFunctionResult(m.id(), 2000);
        REQUIRE(m.returnvalue() == 0);
    }
}
}
