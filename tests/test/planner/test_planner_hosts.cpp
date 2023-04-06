#include <catch2/catch.hpp>

#include "faabric_utils.h"
#include "fixtures.h"

#include <faabric/planner/PlannerClient.h>
#include <faabric/planner/planner.pb.h>
#include <faabric/util/json.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>

#define ASYNC_TIMEOUT_MS 500

using namespace faabric::planner;

namespace tests {
class PlannerTestFixture : public ConfTestFixture
{
  public:
    PlannerTestFixture()
    {
        // Ensure the server is reachable
        cli.ping();
    }

    ~PlannerTestFixture() { resetPlanner(); }

  protected:
    PlannerClient cli;

    void resetPlanner()
    {
        HttpMessage msg;
        msg.set_type(HttpMessage_Type_RESET);
        std::string jsonStr;
        faabric::util::messageToJsonPb(msg, &jsonStr);

        std::pair<int, std::string> result =
          postToUrl(conf.plannerHost, conf.plannerPort, jsonStr);
        assert(result.first == 200);
    }

    // TODO: consider setting a short planner config timeout for the tests
    PlannerConfig getPlannerConfig()
    {
        HttpMessage msg;
        msg.set_type(HttpMessage_Type_GET_CONFIG);
        std::string jsonStr;
        faabric::util::messageToJsonPb(msg, &jsonStr);

        std::pair<int, std::string> result =
          postToUrl(conf.plannerHost, conf.plannerPort, jsonStr);
        REQUIRE(result.first == 200);

        // Check that we can de-serialise the config. Note that if there's a
        // de-serialisation the method will throw an exception
        PlannerConfig config;
        faabric::util::jsonToMessagePb(result.second, &config);
        return config;
    }
};

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
    std::pair<int, int> retVal;

    REQUIRE_NOTHROW(retVal = cli.registerHost(regReq));

    // A call to register a host returns the keep-alive timeout and the unique
    // host id
    REQUIRE(retVal.first > 0);
    REQUIRE(retVal.second > 0);

    // If we send a register request again without setting our host id, the
    // planner will return an error as it already knows the IP, but the host
    // ids don't match
    REQUIRE_THROWS(retVal = cli.registerHost(regReq));

    // If we send a register request with the right host id, it will overwrite
    // the timestamp but the return value will be the same
    std::pair<int, int> newRetVal;
    regReq->mutable_host()->set_hostid(retVal.second);
    REQUIRE_NOTHROW(newRetVal = cli.registerHost(regReq));
    REQUIRE(retVal.first == newRetVal.first);
    REQUIRE(retVal.second == newRetVal.second);

    // Lastly, if we try to register after the host timeout has expired, it
    // will work, but will give us a different id
    std::pair<int, int> diffRetVal;
    int timeToSleep = getPlannerConfig().hosttimeout() * 2;
    SPDLOG_INFO(
      "Sleeping for {} seconds (twice the timeout) to ensure entries expire",
      timeToSleep);
    SLEEP_MS(timeToSleep * 1000);
    REQUIRE_NOTHROW(diffRetVal = cli.registerHost(regReq));
    // Same timeout
    REQUIRE(retVal.first == diffRetVal.first);
    // Different host id
    REQUIRE(retVal.second != diffRetVal.second);
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
    std::pair<int, int> retVal = cli.registerHost(regReq);
    int hostId1 = retVal.second;

    availableHosts = cli.getAvailableHosts();
    REQUIRE(availableHosts.size() == 1);
    REQUIRE(availableHosts.at(0).hostid() == hostId1);

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
    std::pair<int, int> retVal = cli.registerHost(regReq);
    thisHost.set_hostid(retVal.second);

    std::vector<Host> availableHosts = cli.getAvailableHosts();
    REQUIRE(availableHosts.size() == 1);

    auto remReq = std::make_shared<faabric::planner::RemoveHostRequest>();
    *remReq->mutable_host() = thisHost;
    cli.removeHost(remReq);
    // Give time for the async request to go through
    SLEEP_MS(ASYNC_TIMEOUT_MS);
    availableHosts = cli.getAvailableHosts();
    REQUIRE(availableHosts.empty());
}
}
