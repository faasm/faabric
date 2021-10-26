#include <catch.hpp>

#include "faabric_utils.h"

#include <sys/mman.h>

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/transport/PointToPointServer.h>
#include <faabric/util/config.h>
#include <faabric/util/func.h>
#include <faabric/util/macros.h>
#include <faabric/util/scheduling.h>

using namespace faabric::transport;
using namespace faabric::util;

namespace tests {

TEST_CASE_METHOD(PointToPointClientServerFixture,
                 "Test sending point-to-point mappings from client",
                 "[transport][ptp]")
{
    int appIdA = 123;
    int groupIdA = 321;
    int appIdB = 345;
    int groupIdB = 543;

    // Deliberately overlap these indexes to check that the app and group IDs
    // matter
    int appIdxA1 = 1;
    int appIdxA2 = 2;
    int appIdxB1 = 1;

    int groupIdxA1 = 3;
    int groupIdxA2 = 4;
    int groupIdxB1 = 3;

    std::string hostA = "host-a";
    std::string hostB = "host-b";

    REQUIRE(broker.getIdxsRegisteredForGroup(appIdA).empty());
    REQUIRE(broker.getIdxsRegisteredForGroup(appIdB).empty());

    faabric::PointToPointMappings mappingsA;
    mappingsA.set_appid(appIdA);
    mappingsA.set_groupid(groupIdA);

    faabric::PointToPointMappings mappingsB;
    mappingsB.set_appid(appIdB);
    mappingsB.set_groupid(groupIdB);

    auto* mappingA1 = mappingsA.add_mappings();
    mappingA1->set_appidx(appIdxA1);
    mappingA1->set_groupidx(groupIdxA1);
    mappingA1->set_host(hostA);

    auto* mappingA2 = mappingsA.add_mappings();
    mappingA2->set_appidx(appIdxA2);
    mappingA2->set_groupidx(groupIdxA2);
    mappingA2->set_host(hostB);

    auto* mappingB1 = mappingsB.add_mappings();
    mappingB1->set_appidx(appIdxB1);
    mappingB1->set_groupidx(groupIdxB1);
    mappingB1->set_host(hostA);

    cli.sendMappings(mappingsA);
    cli.sendMappings(mappingsB);

    REQUIRE(broker.getIdxsRegisteredForGroup(groupIdA).size() == 2);
    REQUIRE(broker.getIdxsRegisteredForGroup(groupIdB).size() == 1);

    REQUIRE(broker.getHostForReceiver(groupIdA, groupIdxA1) == hostA);
    REQUIRE(broker.getHostForReceiver(groupIdA, groupIdxA2) == hostB);
    REQUIRE(broker.getHostForReceiver(groupIdB, groupIdxB1) == hostA);
}

TEST_CASE_METHOD(PointToPointClientServerFixture,
                 "Test send and receive point-to-point messages",
                 "[transport][ptp]")
{
    int appId = 123;
    int groupId = 345;
    int idxA = 5;
    int idxB = 10;

    // Ensure this host is set to localhost
    faabric::util::SystemConfig& conf = faabric::util::getSystemConfig();
    conf.endpointHost = LOCALHOST;

    // Register both indexes on this host
    faabric::util::SchedulingDecision decision(appId, groupId);

    faabric::Message msgA = faabric::util::messageFactory("foo", "bar");
    msgA.set_appid(appId);
    msgA.set_groupid(groupId);
    msgA.set_groupidx(idxA);

    faabric::Message msgB = faabric::util::messageFactory("foo", "bar");
    msgB.set_appid(appId);
    msgB.set_groupid(groupId);
    msgB.set_groupidx(idxB);

    decision.addMessage(LOCALHOST, msgA);
    decision.addMessage(LOCALHOST, msgB);

    broker.setAndSendMappingsFromSchedulingDecision(decision);

    std::vector<uint8_t> sentDataA = { 0, 1, 2, 3 };
    std::vector<uint8_t> receivedDataA;
    std::vector<uint8_t> sentDataB = { 3, 4, 5 };
    std::vector<uint8_t> receivedDataB;

    // Make sure we send the message before a receiver is available to check
    // async handling
    broker.sendMessage(appId, idxA, idxB, sentDataA.data(), sentDataA.size());

    std::thread t([appId, idxA, idxB, &receivedDataA, &sentDataB] {
        PointToPointBroker& broker = getPointToPointBroker();

        // Receive the first message
        receivedDataA = broker.recvMessage(appId, idxA, idxB);

        // Send a message back
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

TEST_CASE_METHOD(
  PointToPointClientServerFixture,
  "Test setting up point-to-point mappings with scheduling decision",
  "[transport][ptp]")
{
    faabric::util::setMockMode(true);

    int appId = 111;
    int groupId = 222;

    std::string hostA = "hostA";
    std::string hostB = "hostB";
    std::string hostC = "hostC";

    int nMessages = 6;
    auto req = batchExecFactory("foo", "bar", nMessages);
    for (int i = 0; i < nMessages; i++) {
        faabric::Message& m = req->mutable_messages()->at(i);

        m.set_appid(appId);
        m.set_groupid(groupId);
        m.set_appidx(i);
        m.set_groupidx(i + 2);
    }

    faabric::Message& msgA = req->mutable_messages()->at(0);
    faabric::Message& msgB = req->mutable_messages()->at(1);
    faabric::Message& msgC = req->mutable_messages()->at(2);
    faabric::Message& msgD = req->mutable_messages()->at(3);
    faabric::Message& msgE = req->mutable_messages()->at(4);
    faabric::Message& msgF = req->mutable_messages()->at(5);

    SchedulingDecision decision(appId, groupId);
    decision.addMessage(hostB, msgA);
    decision.addMessage(hostA, msgB);
    decision.addMessage(hostC, msgC);
    decision.addMessage(hostB, msgD);
    decision.addMessage(hostB, msgE);
    decision.addMessage(hostC, msgF);

    // Set up and send the mappings
    broker.setAndSendMappingsFromSchedulingDecision(decision);

    // Check locally
    REQUIRE(broker.getHostForReceiver(appId, msgA.groupidx()) == hostB);
    REQUIRE(broker.getHostForReceiver(appId, msgB.groupidx()) == hostA);
    REQUIRE(broker.getHostForReceiver(appId, msgC.groupidx()) == hostC);
    REQUIRE(broker.getHostForReceiver(appId, msgD.groupidx()) == hostB);
    REQUIRE(broker.getHostForReceiver(appId, msgE.groupidx()) == hostB);
    REQUIRE(broker.getHostForReceiver(appId, msgF.groupidx()) == hostC);

    // Check the mappings have been sent out to the relevant hosts
    auto actualSent = getSentMappings();
    REQUIRE(actualSent.size() == 3);

    // Sort the sent mappings based on host
    std::sort(actualSent.begin(),
              actualSent.end(),
              [](const std::pair<std::string, faabric::PointToPointMappings>& a,
                 const std::pair<std::string, faabric::PointToPointMappings>& b)
                -> bool { return a.first < b.first; });

    std::vector<std::string> expectedHosts = { hostA, hostB, hostC };
    std::set<int> expectedAppIdxsA = { msgB.appidx() };
    std::set<int> expectedAppIdxsB = { msgA.appidx(),
                                       msgD.appidx(),
                                       msgE.appidx() };
    std::set<int> expectedAppIdxsC = { msgC.appidx(), msgF.appidx() };

    std::set<int> expectedGroupIdxsA = { msgB.groupidx() };
    std::set<int> expectedGroupIdxsB = { msgA.groupidx(),
                                         msgD.groupidx(),
                                         msgE.groupidx() };
    std::set<int> expectedGroupIdxsC = { msgC.groupidx(), msgF.groupidx() };

    // Check all mappings are the same
    for (int i = 0; i < 3; i++) {
        REQUIRE(actualSent.at(i).first == expectedHosts.at(i));
        faabric::PointToPointMappings actual = actualSent.at(i).second;

        std::set<std::int32_t> appIdxsA;
        std::set<std::int32_t> appIdxsB;
        std::set<std::int32_t> appIdxsC;

        std::set<std::int32_t> groupIdxsA;
        std::set<std::int32_t> groupIdxsB;
        std::set<std::int32_t> groupIdxsC;

        for (const auto& m : actual.mappings()) {
            if (m.host() == hostA) {
                appIdxsA.insert(m.appidx());
                groupIdxsA.insert(m.groupidx());
            } else if (m.host() == hostB) {
                appIdxsB.insert(m.appidx());
                groupIdxsB.insert(m.groupidx());
            } else if (m.host() == hostC) {
                appIdxsC.insert(m.appidx());
                groupIdxsC.insert(m.groupidx());
            } else {
                FAIL();
            }
        }

        REQUIRE(appIdxsA == expectedAppIdxsA);
        REQUIRE(appIdxsB == expectedAppIdxsB);
        REQUIRE(appIdxsC == expectedAppIdxsC);

        REQUIRE(groupIdxsA == expectedGroupIdxsA);
        REQUIRE(groupIdxsB == expectedGroupIdxsB);
        REQUIRE(groupIdxsC == expectedGroupIdxsC);
    }
}

TEST_CASE_METHOD(PointToPointClientServerFixture,
                 "Test waiting for point-to-point messaging to be enabled",
                 "[transport][ptp]")
{
    int appId = 123;
    int groupId = 345;
    std::atomic<int> sharedInt = 5;

    faabric::util::SchedulingDecision decision(appId, groupId);
    faabric::Message msg = faabric::util::messageFactory("foo", "bar");
    decision.addMessage(faabric::util::getSystemConfig().endpointHost, msg);

    // Background thread that will eventually enable the app and change the
    // shared integer
    std::thread t([this, &decision, &sharedInt] {
        SLEEP_MS(1000);
        broker.setUpLocalMappingsFromSchedulingDecision(decision);

        sharedInt.fetch_add(100);
    });

    broker.waitForMappingsOnThisHost(appId);

    // The sum won't have happened yet if this thread hasn't been forced to wait
    REQUIRE(sharedInt == 105);

    // Call again and check it doesn't block
    broker.waitForMappingsOnThisHost(groupId);

    if (t.joinable()) {
        t.join();
    }
}

TEST_CASE_METHOD(PointToPointClientServerFixture,
                 "Test distributed lock/ unlock",
                 "[ptp]")
{
    int groupId = 123;
    int groupSize = 2;

    std::string thisHost = faabric::util::getSystemConfig().endpointHost;

    faabric::Message msg = faabric::util::messageFactory("foo", "bar");

    msg.set_groupsize(groupSize);
    msg.set_groupid(groupId);

    faabric::util::SchedulingDecision decision(msg.appid(), groupId);
    decision.addMessage(thisHost, msg);

    broker.setUpLocalMappingsFromSchedulingDecision(decision);

    bool recursive = false;
    int nCalls = 1;

    SECTION("Recursive")
    {
        recursive = true;
        nCalls = 10;
    }

    SECTION("Non-recursive")
    {
        recursive = false;
        nCalls = 1;
    }

    auto group = PointToPointGroup::getGroup(groupId);
    REQUIRE(group->getLockOwner(recursive) == -1);

    for (int i = 0; i < nCalls; i++) {
        cli.groupLock(msg.groupid(), msg.appidx(), recursive);
        broker.recvMessage(groupId, 0, msg.appidx());
    }

    REQUIRE(group->getLockOwner(recursive) == msg.appidx());

    for (int i = 0; i < nCalls; i++) {
        server.setRequestLatch();
        cli.groupUnlock(msg.groupid(), msg.appidx(), recursive);
        server.awaitRequestLatch();
    }

    REQUIRE(group->getLockOwner(recursive) == -1);
}
}
