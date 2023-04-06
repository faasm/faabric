#include <catch2/catch.hpp>

#include <faabric/planner/planner.pb.h>
#include <faabric/planner/PlannerClient.h>

using namespace faabric::planner;

namespace tests {
class PlannerTestFixture
{
  public:
    PlannerTestFixture()
    {
        // Ensure the server is reachable
        cli.ping();
    }

    ~PlannerTestFixture()
    {
        // TODO - don't expose it as an API method, rather an HTTP endpoint
        // PlannerServer::reset();
    }

  protected:
    PlannerClient cli;
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
    // TODO
}

TEST_CASE_METHOD(PlannerTestFixture, "Test getting the available hosts", "[planner]")
{
    // We can ask for the number of available hosts even if no host has been
    // registered, initially there's 0 available hosts
    std::vector<faabric::planner::Host> availableHosts;
    REQUIRE_NOTHROW(availableHosts = cli.getAvailableHosts());
    REQUIRE(availableHosts.size() == 0);

    // Registering one host increases the count by one
    auto regReq = std::make_shared<faabric::planner::RegisterHostRequest>();
    regReq->mutable_host()->set_ip("foo");
    regReq->mutable_host()->set_slots(12);
    std::pair<int, int> retVal = cli.registerHost(regReq);
    int hostId1 = retVal.second;

    REQUIRE_NOTHROW(availableHosts = cli.getAvailableHosts());
}
}
