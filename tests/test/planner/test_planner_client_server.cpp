#include <catch2/catch.hpp>

#include "fixtures.h"

#include <faabric/planner/PlannerClient.h>
#include <faabric/planner/planner.pb.h>
#include <faabric/util/json.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>

using namespace faabric::planner;

namespace tests {
TEST_CASE_METHOD(PlannerClientServerTestFixture,
                 "Test sending ping to planner",
                 "[planner]")
{
    PlannerClient cli(LOCALHOST);
    REQUIRE_NOTHROW(cli.ping());
}

TEST_CASE_METHOD(PlannerClientServerTestFixture,
                 "Test registering host",
                 "[planner]")
{
    auto regReq = std::make_shared<faabric::planner::RegisterHostRequest>();
    regReq->mutable_host()->set_ip("foo");
    regReq->mutable_host()->set_slots(12);
    int plannerTimeout;

    REQUIRE_NOTHROW(plannerTimeout = plannerCli.registerHost(regReq));

    // A call to register a host returns the keep-alive timeout
    REQUIRE(plannerTimeout > 0);

    // We can register the host again, and get the same timeout
    int newTimeout;
    REQUIRE_NOTHROW(newTimeout = plannerCli.registerHost(regReq));
    REQUIRE(newTimeout == plannerTimeout);
}

TEST_CASE_METHOD(PlannerClientServerTestFixture,
                 "Test getting the available hosts",
                 "[planner]")
{
    // We can ask for the number of available hosts even if no host has been
    // registered, initially there's 0 available hosts
    std::vector<faabric::planner::Host> availableHosts;
    REQUIRE_NOTHROW(availableHosts = plannerCli.getAvailableHosts());
    REQUIRE(availableHosts.empty());

    // Registering one host increases the count by one
    auto regReq = std::make_shared<faabric::planner::RegisterHostRequest>();
    regReq->mutable_host()->set_ip("foo");
    regReq->mutable_host()->set_slots(12);
    plannerCli.registerHost(regReq);

    availableHosts = plannerCli.getAvailableHosts();
    REQUIRE(availableHosts.size() == 1);

    // If we wait more than the timeout, the host will have expired. We sleep
    // for twice the timeout
    int timeToSleep = getPlanner().getConfig().hosttimeout() * 2;
    SPDLOG_INFO(
      "Sleeping for {} seconds (twice the timeout) to ensure entries expire",
      timeToSleep);
    SLEEP_MS(timeToSleep * 1000);
    availableHosts = plannerCli.getAvailableHosts();
    REQUIRE(availableHosts.empty());
}

TEST_CASE_METHOD(PlannerClientServerTestFixture,
                 "Test removing a host",
                 "[planner]")
{
    Host thisHost;
    thisHost.set_ip("foo");
    thisHost.set_slots(12);

    auto regReq = std::make_shared<faabric::planner::RegisterHostRequest>();
    *regReq->mutable_host() = thisHost;
    plannerCli.registerHost(regReq);

    std::vector<Host> availableHosts = plannerCli.getAvailableHosts();
    REQUIRE(availableHosts.size() == 1);

    auto remReq = std::make_shared<faabric::planner::RemoveHostRequest>();
    *remReq->mutable_host() = thisHost;
    plannerCli.removeHost(remReq);
    availableHosts = plannerCli.getAvailableHosts();
    REQUIRE(availableHosts.empty());
}

TEST_CASE_METHOD(PlannerClientServerTestFixture,
                 "Test setting/getting message results",
                 "[planner]")
{
    faabric::util::setMockMode(true);
    auto msgPtr = std::make_shared<faabric::Message>(faabric::util::messageFactory("foo", "bar"));

    // If we try to get the message result before setting it first, nothing
    // happens
    auto resultMsgPtr = plannerCli.getMessageResult(msgPtr);
    REQUIRE(resultMsgPtr == nullptr);

    // If we set the message result, then we can get it
    int expectedReturnValue = 1337;
    msgPtr->set_returnvalue(expectedReturnValue);
    plannerCli.setMessageResult(msgPtr);
    resultMsgPtr = plannerCli.getMessageResult(msgPtr);
    REQUIRE(resultMsgPtr->id() == msgPtr->id());
    REQUIRE(resultMsgPtr->appid() == msgPtr->appid());
    REQUIRE(resultMsgPtr->returnvalue() == expectedReturnValue);

    // Also, setting the message result triggers the planner to send a
    // request to the host that tried to get the result before
    auto msgResults = faabric::scheduler::getMessageResults();
    REQUIRE(msgResults.size() == 1);
    REQUIRE(msgResults.at(0).first == faabric::util::getSystemConfig().endpointHost);

    faabric::scheduler::clearMockRequests();
    faabric::util::setMockMode(false);

    // Shutdown the scheduler as we are using the function client/server (even
    // if mocked)
    faabric::scheduler::getScheduler().shutdown();
}
}
