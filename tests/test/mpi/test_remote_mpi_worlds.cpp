#include <catch2/catch.hpp>

#include "faabric_utils.h"
#include "fixtures.h"

#include <faabric/mpi/MpiWorldRegistry.h>
#include <faabric/mpi/mpi.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/bytes.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>

#include <thread>

// The tests in this file are used to test the internal behaviour of MPI when
// running in a distributed behaviour. They should test very specific things
// _always_ in mocking mode. For truly multi-host MPI tests you must write
// an actual distributed test.

using namespace faabric::mpi;
using namespace faabric::scheduler;

namespace tests {
std::set<int> getReceiversFromMessages(std::vector<MpiMessage> msgs)
{
    std::set<int> receivers;
    for (const auto& msg : msgs) {
        receivers.insert(msg.recvRank);
    }

    return receivers;
}

TEST_CASE_METHOD(RemoteMpiTestFixture,
                 "Test number of messages sent during broadcast",
                 "[mpi]")
{
    setWorldSizes(4, 2, 2);

    // Init worlds
    MpiWorld& thisWorld = getMpiWorldRegistry().createWorld(msg, worldId);
    otherWorld.initialiseFromMsg(msg);

    // Call broadcast and check sent messages
    std::set<int> expectedRecvRanks;
    int expectedNumMsg;
    int sendRank;
    int recvRank;

    SECTION("Check broadcast from sender and sender is a local leader")
    {
        recvRank = 0;
        sendRank = recvRank;
        expectedNumMsg = 2;
        expectedRecvRanks = { 1, 2 };
    }

    SECTION("Check broadcast from sender but sender is not a local leader")
    {
        recvRank = 1;
        sendRank = recvRank;
        expectedNumMsg = 2;
        expectedRecvRanks = { 0, 2 };
    }

    SECTION("Check broadcast from a rank that is not the sender, is not a "
            "leader, but is colocated with the sender")
    {
        recvRank = 0;
        sendRank = 1;
        expectedNumMsg = 0;
        expectedRecvRanks = {};
    }

    SECTION("Check broadcast from a rank that is not the sender, is a leader, "
            "and is colocated with the sender")
    {
        recvRank = 1;
        sendRank = 0;
        expectedNumMsg = 0;
        expectedRecvRanks = {};
    }

    SECTION(
      "Check broadcast from a rank not colocated with sender, but local leader")
    {
        recvRank = 2;
        sendRank = 0;
        expectedNumMsg = 1;
        expectedRecvRanks = { 3 };
    }

    SECTION("Check broadcast from a rank not colocated with sender, and not "
            "local leader")
    {
        recvRank = 3;
        sendRank = 0;
        expectedNumMsg = 0;
        expectedRecvRanks = {};
    }

    // Check for root
    std::vector<int> messageData = { 0, 1, 2 };
    if (recvRank < 2) {
        thisWorld.broadcast(sendRank,
                            recvRank,
                            BYTES(messageData.data()),
                            MPI_INT,
                            messageData.size(),
                            MpiMessageType::BROADCAST);
    } else {
        otherWorld.broadcast(sendRank,
                             recvRank,
                             BYTES(messageData.data()),
                             MPI_INT,
                             messageData.size(),
                             MpiMessageType::BROADCAST);
    }
    auto msgs = getMpiMockedMessages(recvRank);
    REQUIRE(msgs.size() == expectedNumMsg);
    REQUIRE(getReceiversFromMessages(msgs) == expectedRecvRanks);

    faabric::util::setMockMode(false);
    otherWorld.destroy();
    thisWorld.destroy();
}

TEST_CASE_METHOD(RemoteMpiTestFixture,
                 "Test number of messages sent during reduce",
                 "[mpi]")
{
    setWorldSizes(4, 2, 2);

    // Init worlds
    MpiWorld& thisWorld = getMpiWorldRegistry().createWorld(msg, worldId);
    otherWorld.initialiseFromMsg(msg);

    std::set<int> expectedSentMsgRanks;
    int expectedNumMsgSent;
    int sendRank;
    int recvRank;

    SECTION("Call reduce from receiver (local), and receiver is local leader")
    {
        recvRank = 0;
        sendRank = recvRank;
        expectedNumMsgSent = 0;
        expectedSentMsgRanks = {};
    }

    SECTION(
      "Call reduce from receiver (local), and receiver is non-local leader")
    {
        recvRank = 1;
        sendRank = recvRank;
        expectedNumMsgSent = 0;
        expectedSentMsgRanks = {};
    }

    SECTION("Call reduce from non-receiver, colocated with receiver, and local "
            "leader")
    {
        recvRank = 1;
        sendRank = 0;
        expectedNumMsgSent = 1;
        expectedSentMsgRanks = { recvRank };
    }

    SECTION("Call reduce from non-receiver, colocated with receiver")
    {
        recvRank = 0;
        sendRank = 1;
        expectedNumMsgSent = 1;
        expectedSentMsgRanks = { recvRank };
    }

    SECTION("Call reduce from non-receiver rank, not colocated with receiver, "
            "but local leader")
    {
        recvRank = 0;
        sendRank = 2;
        expectedNumMsgSent = 1;
        expectedSentMsgRanks = { recvRank };
    }

    SECTION("Call reduce from non-receiver rank, not colocated with receiver")
    {
        recvRank = 0;
        sendRank = 3;
        expectedNumMsgSent = 1;
        expectedSentMsgRanks = { 2 };
    }

    std::vector<int> messageData = { 0, 1, 2 };
    std::vector<int> recvData(messageData.size());
    if (sendRank < 2) {
        thisWorld.reduce(sendRank,
                         recvRank,
                         BYTES(messageData.data()),
                         BYTES(recvData.data()),
                         MPI_INT,
                         messageData.size(),
                         MPI_SUM);
    } else {
        otherWorld.reduce(sendRank,
                          recvRank,
                          BYTES(messageData.data()),
                          BYTES(recvData.data()),
                          MPI_INT,
                          messageData.size(),
                          MPI_SUM);
    }
    auto msgs = getMpiMockedMessages(sendRank);
    REQUIRE(msgs.size() == expectedNumMsgSent);
    REQUIRE(getReceiversFromMessages(msgs) == expectedSentMsgRanks);

    otherWorld.destroy();
    thisWorld.destroy();
}

std::set<int> getMsgCountsFromMessages(std::vector<MpiMessage> msgs)
{
    std::set<int> counts;
    for (const auto& msg : msgs) {
        counts.insert(msg.count);
    }

    return counts;
}

TEST_CASE_METHOD(RemoteMpiTestFixture,
                 "Test number of messages sent during gather",
                 "[mpi]")
{
    int worldSize = 4;
    setWorldSizes(worldSize, 2, 2);
    std::vector<int> messageData = { 0, 1, 2 };
    int nPerRank = messageData.size();

    // Init worlds
    MpiWorld& thisWorld = getMpiWorldRegistry().createWorld(msg, worldId);
    otherWorld.initialiseFromMsg(msg);

    std::set<int> expectedSentMsgRanks;
    std::set<int> expectedSentMsgCounts;
    int expectedNumMsgSent;
    int sendRank;
    int recvRank;

    SECTION("Call gather from receiver (local), and receiver is local leader")
    {
        recvRank = 0;
        sendRank = recvRank;
        expectedNumMsgSent = 0;
        expectedSentMsgRanks = {};
        expectedSentMsgCounts = {};
    }

    SECTION(
      "Call gather from receiver (local), and receiver is non-local leader")
    {
        recvRank = 1;
        sendRank = recvRank;
        expectedNumMsgSent = 0;
        expectedSentMsgRanks = {};
        expectedSentMsgCounts = {};
    }

    SECTION("Call gather from non-receiver, colocated with receiver, and local "
            "leader")
    {
        recvRank = 1;
        sendRank = 0;
        expectedNumMsgSent = 1;
        expectedSentMsgRanks = { recvRank };
        expectedSentMsgCounts = { nPerRank };
    }

    SECTION("Call gather from non-receiver, colocated with receiver")
    {
        recvRank = 0;
        sendRank = 1;
        expectedNumMsgSent = 1;
        expectedSentMsgRanks = { recvRank };
        expectedSentMsgCounts = { nPerRank };
    }

    SECTION("Call gather from non-receiver rank, not colocated with receiver, "
            "but local leader")
    {
        recvRank = 0;
        sendRank = 2;
        expectedNumMsgSent = 1;
        expectedSentMsgRanks = { recvRank };
        expectedSentMsgCounts = { 2 * nPerRank };
    }

    SECTION("Call gather from non-receiver rank, not colocated with receiver")
    {
        recvRank = 0;
        sendRank = 3;
        expectedNumMsgSent = 1;
        expectedSentMsgRanks = { 2 };
        expectedSentMsgCounts = { nPerRank };
    }

    std::vector<int> gatherData(worldSize * nPerRank);
    if (sendRank < 2) {
        thisWorld.gather(sendRank,
                         recvRank,
                         BYTES(messageData.data()),
                         MPI_INT,
                         nPerRank,
                         BYTES(gatherData.data()),
                         MPI_INT,
                         nPerRank);
    } else {
        otherWorld.gather(sendRank,
                          recvRank,
                          BYTES(messageData.data()),
                          MPI_INT,
                          nPerRank,
                          BYTES(gatherData.data()),
                          MPI_INT,
                          nPerRank);
    }
    auto msgs = getMpiMockedMessages(sendRank);
    REQUIRE(msgs.size() == expectedNumMsgSent);
    REQUIRE(getReceiversFromMessages(msgs) == expectedSentMsgRanks);
    REQUIRE(getMsgCountsFromMessages(msgs) == expectedSentMsgCounts);

    otherWorld.destroy();
    thisWorld.destroy();
}
}
