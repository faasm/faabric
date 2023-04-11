#include <catch2/catch.hpp>

#include "fixtures.h"

#include <faabric/planner/PlannerClient.h>
#include <faabric/planner/planner.pb.h>
#include <faabric/util/json.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>

#define ASYNC_TIMEOUT_MS 500

using namespace faabric::planner;

namespace tests {
TEST_CASE("Test basic planner client operations", "[planner]")
{
    PlannerClient cli;

    REQUIRE_NOTHROW(cli.ping());
}

TEST_CASE_METHOD(PlannerTestFixture, "Test registering host", "[planner]")
{
    auto regReq = std::make_shared<faabric::planner::RegisterHostRequest>();
    regReq->mutable_host()->set_ip("foo");
    regReq->mutable_host()->set_slots(12);
    int plannerTimeout;

    REQUIRE_NOTHROW(plannerTimeout = cli.registerHost(regReq));

    // A call to register a host returns the keep-alive timeout
    REQUIRE(plannerTimeout > 0);

    // We can register the host again, and get the same timeout
    int newTimeout;
    REQUIRE_NOTHROW(newTimeout = cli.registerHost(regReq));
    REQUIRE(newTimeout == plannerTimeout);
}

TEST_CASE_METHOD(PlannerTestFixture,
                 "Test getting the available hosts",
                 "[planner]")
{
    // We can ask for the number of available hosts even if no host has been
    // registered, initially there's 0 available hosts
    std::vector<faabric::planner::Host> availableHosts;
    REQUIRE_NOTHROW(availableHosts = cli.getAvailableHosts());
    REQUIRE(availableHosts.empty());

    // Registering one host increases the count by one
    auto regReq = std::make_shared<faabric::planner::RegisterHostRequest>();
    regReq->mutable_host()->set_ip("foo");
    regReq->mutable_host()->set_slots(12);
    cli.registerHost(regReq);

    availableHosts = cli.getAvailableHosts();
    REQUIRE(availableHosts.size() == 1);

    // If we wait more than the timeout, the host will have expired. We sleep
    // for twice the timeout
    int timeToSleep = getPlannerConfig().hosttimeout() * 2;
    SPDLOG_INFO(
      "Sleeping for {} seconds (twice the timeout) to ensure entries expire",
      timeToSleep);
    SLEEP_MS(timeToSleep * 1000);
    availableHosts = cli.getAvailableHosts();
    REQUIRE(availableHosts.empty());
}

TEST_CASE_METHOD(PlannerTestFixture, "Test removing a host", "[planner]")
{
    Host thisHost;
    thisHost.set_ip("foo");
    thisHost.set_slots(12);

    auto regReq = std::make_shared<faabric::planner::RegisterHostRequest>();
    *regReq->mutable_host() = thisHost;
    cli.registerHost(regReq);

    std::vector<Host> availableHosts = cli.getAvailableHosts();
    REQUIRE(availableHosts.size() == 1);

    auto remReq = std::make_shared<faabric::planner::RemoveHostRequest>();
    *remReq->mutable_host() = thisHost;
    cli.removeHost(remReq);
    availableHosts = cli.getAvailableHosts();
    REQUIRE(availableHosts.empty());
}
}
