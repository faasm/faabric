#include "faabric/util/testing.h"
#include <catch.hpp>

#include <faabric_utils.h>

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/transport/PointToPointClient.h>
#include <faabric/transport/PointToPointRegistry.h>
#include <faabric/transport/PointToPointServer.h>

using namespace faabric::transport;

namespace tests {

TEST_CASE_METHOD(PointToPointFixture,
                 "Test sending mappings via registry",
                 "[transport]")
{
    faabric::util::setMockMode(true);

    int appIdA = 123;
    int appIdB = 345;

    int idxA1 = 1;
    int idxA2 = 2;
    int idxB1 = 1;

    std::string hostA = "host-a";
    std::string hostB = "host-b";

    reg.setHostForReceiver(appIdA, idxA1, hostA);
    reg.setHostForReceiver(appIdA, idxA2, hostB);
    reg.setHostForReceiver(appIdB, idxB1, hostB);

    reg.sendMappings(appIdA, "other-host");

    auto actualSent = getSentMappings();
    REQUIRE(actualSent.size() == 1);

    faabric::PointToPointMappings actualMappings = actualSent.at(0).second;
    REQUIRE(actualMappings.mappings().size() == 2);

    faabric::PointToPointMappings::PointToPointMapping mappingA =
      actualMappings.mappings().at(0);
    faabric::PointToPointMappings::PointToPointMapping mappingB =
      actualMappings.mappings().at(1);

    REQUIRE(mappingA.appid() == appIdA);
    REQUIRE(mappingB.appid() == appIdA);

    // Note - we don't know the order the mappings are sent in so we have to
    // check both possibilities
    if (mappingA.recvidx() == idxA1) {
        REQUIRE(mappingA.host() == hostA);

        REQUIRE(mappingB.recvidx() == idxA2);
        REQUIRE(mappingB.host() == hostB);
    } else if (mappingA.recvidx() == idxA2) {
        REQUIRE(mappingA.host() == hostB);

        REQUIRE(mappingB.recvidx() == idxA1);
        REQUIRE(mappingB.host() == hostA);
    } else {
        FAIL();
    }
}

TEST_CASE_METHOD(PointToPointFixture,
                 "Test sending mappings from client",
                 "[transport]")
{
    int appIdA = 123;
    int appIdB = 345;

    int idxA1 = 1;
    int idxA2 = 2;
    int idxB1 = 1;

    std::string hostA = "host-a";
    std::string hostB = "host-b";

    REQUIRE(reg.getIdxsRegisteredForApp(appIdA).empty());
    REQUIRE(reg.getIdxsRegisteredForApp(appIdB).empty());

    faabric::PointToPointMappings mappings;

    auto* mappingA1 = mappings.add_mappings();
    mappingA1->set_appid(appIdA);
    mappingA1->set_recvidx(idxA1);
    mappingA1->set_host(hostA);

    auto* mappingA2 = mappings.add_mappings();
    mappingA2->set_appid(appIdA);
    mappingA2->set_recvidx(idxA2);
    mappingA2->set_host(hostB);

    auto* mappingB1 = mappings.add_mappings();
    mappingB1->set_appid(appIdB);
    mappingB1->set_recvidx(idxB1);
    mappingB1->set_host(hostA);

    cli.sendMappings(mappings);

    REQUIRE(reg.getIdxsRegisteredForApp(appIdA).size() == 2);
    REQUIRE(reg.getIdxsRegisteredForApp(appIdB).size() == 1);

    REQUIRE(reg.getHostForReceiver(appIdA, idxA1) == hostA);
    REQUIRE(reg.getHostForReceiver(appIdA, idxA2) == hostB);
    REQUIRE(reg.getHostForReceiver(appIdB, idxB1) == hostA);
}
}
