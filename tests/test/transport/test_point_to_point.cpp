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
    int appIdB = 345;

    int idxA1 = 1;
    int idxA2 = 2;
    int idxB1 = 1;

    std::string hostA = "host-a";
    std::string hostB = "host-b";

    REQUIRE(broker.getIdxsRegisteredForGroup(appIdA).empty());
    REQUIRE(broker.getIdxsRegisteredForGroup(appIdB).empty());

    faabric::PointToPointMappings mappingsA;
    mappingsA.set_groupid(appIdA);

    faabric::PointToPointMappings mappingsB;
    mappingsB.set_groupid(appIdB);

    auto* mappingA1 = mappingsA.add_mappings();
    mappingA1->set_recvidx(idxA1);
    mappingA1->set_host(hostA);

    auto* mappingA2 = mappingsA.add_mappings();
    mappingA2->set_recvidx(idxA2);
    mappingA2->set_host(hostB);

    auto* mappingB1 = mappingsB.add_mappings();
    mappingB1->set_recvidx(idxB1);
    mappingB1->set_host(hostA);

    cli.sendMappings(mappingsA);
    cli.sendMappings(mappingsB);

    REQUIRE(broker.getIdxsRegisteredForGroup(appIdA).size() == 2);
    REQUIRE(broker.getIdxsRegisteredForGroup(appIdB).size() == 1);

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
    faabric::util::SchedulingDecision decision(appId);

    faabric::Message msgA = faabric::util::messageFactory("foo", "bar");
    msgA.set_appid(appId);
    msgA.set_appindex(idxA);

    faabric::Message msgB = faabric::util::messageFactory("foo", "bar");
    msgB.set_appid(appId);
    msgB.set_appindex(idxB);

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

    std::string hostA = "hostA";
    std::string hostB = "hostB";
    std::string hostC = "hostC";

    int nMessages = 6;
    auto req = batchExecFactory("foo", "bar", nMessages);
    for (int i = 0; i < nMessages; i++) {
        req->mutable_messages()->at(i).set_appindex(i);
    }

    faabric::Message& msgA = req->mutable_messages()->at(0);
    faabric::Message& msgB = req->mutable_messages()->at(1);
    faabric::Message& msgC = req->mutable_messages()->at(2);
    faabric::Message& msgD = req->mutable_messages()->at(3);
    faabric::Message& msgE = req->mutable_messages()->at(4);
    faabric::Message& msgF = req->mutable_messages()->at(5);

    int appId = msgA.appid();
    SchedulingDecision decision(appId);
    decision.addMessage(hostB, msgA);
    decision.addMessage(hostA, msgB);
    decision.addMessage(hostC, msgC);
    decision.addMessage(hostB, msgD);
    decision.addMessage(hostB, msgE);
    decision.addMessage(hostC, msgF);

    // Set up and send the mappings
    broker.setAndSendMappingsFromSchedulingDecision(decision);

    // Check locally
    REQUIRE(broker.getHostForReceiver(appId, msgA.appindex()) == hostB);
    REQUIRE(broker.getHostForReceiver(appId, msgB.appindex()) == hostA);
    REQUIRE(broker.getHostForReceiver(appId, msgC.appindex()) == hostC);
    REQUIRE(broker.getHostForReceiver(appId, msgD.appindex()) == hostB);
    REQUIRE(broker.getHostForReceiver(appId, msgE.appindex()) == hostB);
    REQUIRE(broker.getHostForReceiver(appId, msgF.appindex()) == hostC);

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
    std::set<int> expectedHostAIdxs = { msgB.appindex() };
    std::set<int> expectedHostBIdxs = { msgA.appindex(),
                                        msgD.appindex(),
                                        msgE.appindex() };
    std::set<int> expectedHostCIdxs = { msgC.appindex(), msgF.appindex() };

    // Check all mappings are the same
    for (int i = 0; i < 3; i++) {
        REQUIRE(actualSent.at(i).first == expectedHosts.at(i));
        faabric::PointToPointMappings actual = actualSent.at(i).second;

        std::set<std::int32_t> hostAIdxs;
        std::set<std::int32_t> hostBIdxs;
        std::set<std::int32_t> hostCIdxs;

        for (const auto& m : actual.mappings()) {
            if (m.host() == hostA) {
                hostAIdxs.insert(m.recvidx());
            } else if (m.host() == hostB) {
                hostBIdxs.insert(m.recvidx());
            } else if (m.host() == hostC) {
                hostCIdxs.insert(m.recvidx());
            } else {
                FAIL();
            }
        }

        REQUIRE(hostAIdxs == expectedHostAIdxs);
        REQUIRE(hostBIdxs == expectedHostBIdxs);
        REQUIRE(hostCIdxs == expectedHostCIdxs);
    }
}

TEST_CASE_METHOD(PointToPointClientServerFixture,
                 "Test waiting for point-to-point messaging to be enabled",
                 "[transport][ptp]")
{
    int appId = 123;
    std::atomic<int> sharedInt = 5;

    faabric::util::SchedulingDecision decision(appId);
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
    broker.waitForMappingsOnThisHost(appId);

    if (t.joinable()) {
        t.join();
    }
}

TEST_CASE_METHOD(PointToPointClientServerFixture,
                 "Test distributed lock/ unlock",
                 "[ptp][sync]")
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

    auto group = broker.getGroup(groupId);
    REQUIRE(group->getLockOwner(recursive) == -1);

    for (int i = 0; i < nCalls; i++) {
        cli.groupLock(msg.groupid(), msg.appindex(), recursive);
        broker.recvMessage(groupId, 0, msg.appindex());
    }

    REQUIRE(group->getLockOwner(recursive) == msg.appindex());

    for (int i = 0; i < nCalls; i++) {
        server.setRequestLatch();
        cli.groupUnlock(msg.groupid(), msg.appindex(), recursive);
        server.awaitRequestLatch();
    }

    REQUIRE(group->getLockOwner(recursive) == -1);
}
}
