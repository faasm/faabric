#include <catch.hpp>

#include "faabric/proto/faabric.pb.h"
#include "faabric/scheduler/Scheduler.h"
#include "faabric_utils.h"

#include <sys/mman.h>

#include <faabric/transport/PointToPointBroker.h>
#include <faabric/util/config.h>
#include <faabric/util/macros.h>

using namespace faabric::transport;
using namespace faabric::util;

namespace tests {

class PointToPointClientServerFixture
  : public PointToPointTestFixture
  , SchedulerTestFixture
{
  public:
    PointToPointClientServerFixture()
      : cli(LOCALHOST)
    {
        server.start();
    }

    ~PointToPointClientServerFixture() { server.stop(); }

  protected:
    faabric::transport::PointToPointClient cli;
    faabric::transport::PointToPointServer server;
};

TEST_CASE_METHOD(PointToPointClientServerFixture,
                 "Test set and get point-to-point hosts",
                 "[transport][ptp]")
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

    REQUIRE_THROWS(broker.getHostForReceiver(appIdA, idxA1));
    REQUIRE_THROWS(broker.getHostForReceiver(appIdA, idxA2));
    REQUIRE_THROWS(broker.getHostForReceiver(appIdB, idxB1));
    REQUIRE_THROWS(broker.getHostForReceiver(appIdB, idxB2));

    broker.setHostForReceiver(appIdA, idxA1, hostA);
    broker.setHostForReceiver(appIdB, idxB1, hostB);

    std::set<int> expectedA = { idxA1 };
    std::set<int> expectedB = { idxB1 };
    REQUIRE(broker.getIdxsRegisteredForApp(appIdA) == expectedA);
    REQUIRE(broker.getIdxsRegisteredForApp(appIdB) == expectedB);

    REQUIRE(broker.getHostForReceiver(appIdA, idxA1) == hostA);
    REQUIRE_THROWS(broker.getHostForReceiver(appIdA, idxA2));
    REQUIRE(broker.getHostForReceiver(appIdB, idxB1) == hostB);
    REQUIRE_THROWS(broker.getHostForReceiver(appIdB, idxB2));

    broker.setHostForReceiver(appIdA, idxA2, hostB);
    broker.setHostForReceiver(appIdB, idxB2, hostC);

    expectedA = { idxA1, idxA2 };
    expectedB = { idxB1, idxB2 };

    REQUIRE(broker.getIdxsRegisteredForApp(appIdA) == expectedA);
    REQUIRE(broker.getIdxsRegisteredForApp(appIdB) == expectedB);

    REQUIRE(broker.getHostForReceiver(appIdA, idxA1) == hostA);
    REQUIRE(broker.getHostForReceiver(appIdA, idxA2) == hostB);
    REQUIRE(broker.getHostForReceiver(appIdB, idxB1) == hostB);
    REQUIRE(broker.getHostForReceiver(appIdB, idxB2) == hostC);
}

TEST_CASE_METHOD(PointToPointClientServerFixture,
                 "Test sending point-to-point mappings via broker",
                 "[transport][ptp]")
{
    faabric::util::setMockMode(true);

    int appIdA = 123;
    int appIdB = 345;

    int idxA1 = 1;
    int idxA2 = 2;
    int idxB1 = 1;

    std::string hostA = "host-a";
    std::string hostB = "host-b";
    std::string hostC = "host-c";

    faabric::scheduler::Scheduler& sch = faabric::scheduler::getScheduler();
    sch.reset();

    sch.addHostToGlobalSet(hostA);
    sch.addHostToGlobalSet(hostB);
    sch.addHostToGlobalSet(hostC);

    // Includes this host
    REQUIRE(sch.getAvailableHosts().size() == 4);

    broker.setHostForReceiver(appIdA, idxA1, hostA);
    broker.setHostForReceiver(appIdA, idxA2, hostB);
    broker.setHostForReceiver(appIdB, idxB1, hostB);

    std::vector<std::string> expectedHosts;
    SECTION("Send single host")
    {
        broker.sendMappings(appIdA, hostC);
        expectedHosts = { hostC };
    }

    SECTION("Broadcast all hosts")
    {
        broker.broadcastMappings(appIdA);

        // Don't expect to be broadcast to this host
        expectedHosts = { hostA, hostB, hostC };
    }

    auto actualSent = getSentMappings();
    REQUIRE(actualSent.size() == expectedHosts.size());

    // Sort the sent mappings based on host
    std::sort(actualSent.begin(),
              actualSent.end(),
              [](const std::pair<std::string, faabric::PointToPointMappings>& a,
                 const std::pair<std::string, faabric::PointToPointMappings>& b)
                -> bool { return a.first < b.first; });

    // Check each of the sent mappings is as we would expect
    for (int i = 0; i < expectedHosts.size(); i++) {
        REQUIRE(actualSent.at(i).first == expectedHosts.at(i));

        faabric::PointToPointMappings actualMappings = actualSent.at(i).second;
        REQUIRE(actualMappings.mappings().size() == 2);

        faabric::PointToPointMappings::PointToPointMapping mappingA =
          actualMappings.mappings().at(0);
        faabric::PointToPointMappings::PointToPointMapping mappingB =
          actualMappings.mappings().at(1);

        REQUIRE(mappingA.appid() == appIdA);
        REQUIRE(mappingB.appid() == appIdA);

        // Note - we don't know the order of the mappings and can't easily sort
        // the data in the protobuf object, so it's easiest just to check both
        // possible orderings.
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
}

TEST_CASE_METHOD(PointToPointClientServerFixture,
                 "Test sending point-to-point mappings from client",
                 "[transport][ptp]")
{
    int appIdA = 123;
    int appIdB = 345;

    int idxA1 = 1;
    int idxA2 = 2;
    int idxB1 = 1;

    std::string hostA = "host-a";
    std::string hostB = "host-b";

    REQUIRE(broker.getIdxsRegisteredForApp(appIdA).empty());
    REQUIRE(broker.getIdxsRegisteredForApp(appIdB).empty());

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

    REQUIRE(broker.getIdxsRegisteredForApp(appIdA).size() == 2);
    REQUIRE(broker.getIdxsRegisteredForApp(appIdB).size() == 1);

    REQUIRE(broker.getHostForReceiver(appIdA, idxA1) == hostA);
    REQUIRE(broker.getHostForReceiver(appIdA, idxA2) == hostB);
    REQUIRE(broker.getHostForReceiver(appIdB, idxB1) == hostA);
}

TEST_CASE_METHOD(PointToPointClientServerFixture,
                 "Test send and receive point-to-point messages",
                 "[transport][ptp]")
{
    int appId = 123;
    int idxA = 5;
    int idxB = 10;

    // Ensure this host is set to localhost
    faabric::util::SystemConfig& conf = faabric::util::getSystemConfig();
    conf.endpointHost = LOCALHOST;

    // Register both indexes on this host
    broker.setHostForReceiver(appId, idxA, LOCALHOST);
    broker.setHostForReceiver(appId, idxB, LOCALHOST);

    std::vector<uint8_t> sentDataA = { 0, 1, 2, 3 };
    std::vector<uint8_t> receivedDataA;
    std::vector<uint8_t> sentDataB = { 3, 4, 5 };
    std::vector<uint8_t> receivedDataB;

    // Make sure we send the message before a receiver is available to check
    // async handling
    broker.sendMessage(appId, idxA, idxB, sentDataA.data(), sentDataA.size());

    SLEEP_MS(1000);

    std::thread t([appId, idxA, idxB, &receivedDataA, &sentDataB] {
        PointToPointBroker& broker = getPointToPointBroker();

        // Receive the first message
        receivedDataA = broker.recvMessage(appId, idxA, idxB);

        // Send a message back (note reversing the indexes)
        broker.sendMessage(
          appId, idxB, idxA, sentDataB.data(), sentDataB.size());

        broker.resetThreadLocalCache();
    });

    // Receive the message sent back
    receivedDataB = broker.recvMessage(appId, idxB, idxA);

    if (t.joinable()) {
        t.join();
    }

    REQUIRE(receivedDataA == sentDataA);
    REQUIRE(receivedDataB == sentDataB);

    conf.reset();
}
}
