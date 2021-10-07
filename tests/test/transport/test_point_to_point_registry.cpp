#include "faabric_utils.h"
#include <catch.hpp>

#include <sys/mman.h>

#include <faabric/transport/PointToPointRegistry.h>

using namespace faabric::transport;
using namespace faabric::util;

namespace tests {

class PointToPointFixture
{
  public:
    PointToPointFixture()
      : reg(faabric::transport::getPointToPointRegistry())
    {
        reg.clear();
    }

    ~PointToPointFixture() { reg.clear(); }

  protected:
    faabric::transport::PointToPointRegistry& reg;
};

TEST_CASE_METHOD(PointToPointFixture,
                 "Test set and get point-to-point hosts",
                 "[transport]")
{
    // Note - deliberately overlap app indexes to make sure app id counts
    int appIdA = 123;
    int appIdB = 345;
    int idxA1 = 0;
    int idxB1 = 2;
    int idxA2 = 10;
    int idxB2 = 10;

    std::string hostA = "host-a";
    std::string hostB = "host-b";
    std::string hostC = "host-c";

    REQUIRE_THROWS(reg.getHostForReceiver(appIdA, idxA1));
    REQUIRE_THROWS(reg.getHostForReceiver(appIdA, idxA2));
    REQUIRE_THROWS(reg.getHostForReceiver(appIdB, idxB1));
    REQUIRE_THROWS(reg.getHostForReceiver(appIdB, idxB2));

    reg.setHostForReceiver(appIdA, idxA1, hostA);
    reg.setHostForReceiver(appIdB, idxB1, hostB);

    std::set<int> expectedA = { idxA1 };
    std::set<int> expectedB = { idxB1 };
    REQUIRE(reg.getIdxsRegisteredForApp(appIdA) == expectedA);
    REQUIRE(reg.getIdxsRegisteredForApp(appIdB) == expectedB);

    REQUIRE(reg.getHostForReceiver(appIdA, idxA1) == hostA);
    REQUIRE_THROWS(reg.getHostForReceiver(appIdA, idxA2));
    REQUIRE(reg.getHostForReceiver(appIdB, idxB1) == hostB);
    REQUIRE_THROWS(reg.getHostForReceiver(appIdB, idxB2));

    reg.setHostForReceiver(appIdA, idxA2, hostB);
    reg.setHostForReceiver(appIdB, idxB2, hostC);

    expectedA = { idxA1, idxA2 };
    expectedB = { idxB1, idxB2 };

    REQUIRE(reg.getIdxsRegisteredForApp(appIdA) == expectedA);
    REQUIRE(reg.getIdxsRegisteredForApp(appIdB) == expectedB);

    REQUIRE(reg.getHostForReceiver(appIdA, idxA1) == hostA);
    REQUIRE(reg.getHostForReceiver(appIdA, idxA2) == hostB);
    REQUIRE(reg.getHostForReceiver(appIdB, idxB1) == hostB);
    REQUIRE(reg.getHostForReceiver(appIdB, idxB2) == hostC);
}
}
