#include <catch2/catch.hpp>

#include <faabric/mpi/mpi.h>
#include <faabric/scheduler/MpiWorldRegistry.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/bytes.h>
#include <faabric/util/macros.h>
#include <faabric_utils.h>

#include <faabric/util/logging.h>

#include <thread>

using namespace faabric::scheduler;

namespace tests {
class RemoteCollectiveTestFixture : public RemoteMpiTestFixture
{
  public:
    RemoteCollectiveTestFixture()
    {
        thisWorldRanks = { thisHostRankB, thisHostRankA, 0 };
        otherWorldRanks = { otherHostRankB, otherHostRankC, otherHostRankA };

        setWorldSizes(thisWorldSize, 3, 3);
    }

    MpiWorld& setUpThisWorld()
    {
        MpiWorld& thisWorld = getMpiWorldRegistry().createWorld(msg, worldId);
        faabric::util::setMockMode(false);
        thisWorld.broadcastHostsToRanks();

        // Check it's set up as we expect
        for (auto r : otherWorldRanks) {
            REQUIRE(thisWorld.getHostForRank(r) == otherHost);
        }

        for (auto r : thisWorldRanks) {
            REQUIRE(thisWorld.getHostForRank(r) == thisHost);
        }

        return thisWorld;
    }

  protected:
    int thisWorldSize = 6;

    int thisHostRankA = 1;
    int thisHostRankB = 2;
    int otherHostRankA = 3;
    int otherHostRankB = 4;
    int otherHostRankC = 5;

    std::vector<int> otherWorldRanks;
    std::vector<int> thisWorldRanks;
};

TEST_CASE_METHOD(RemoteMpiTestFixture, "Test rank allocation", "[mpi]")
{
    // Allocate two ranks in total, one rank per host
    setWorldSizes(4, 2, 2);

    MpiWorld& thisWorld = getMpiWorldRegistry().createWorld(msg, worldId);
    faabric::util::setMockMode(false);
    thisWorld.broadcastHostsToRanks();

    // Background thread to receive the allocation
    std::thread otherWorldThread([this] {
        otherWorld.initialiseFromMsg(msg);

        assert(otherWorld.getHostForRank(0) == thisHost);
        assert(otherWorld.getHostForRank(1) == thisHost);
        assert(otherWorld.getHostForRank(2) == otherHost);
        assert(otherWorld.getHostForRank(3) == otherHost);

        otherWorld.destroy();
    });

    if (otherWorldThread.joinable()) {
        otherWorldThread.join();
    }

    REQUIRE(thisWorld.getHostForRank(0) == thisHost);
    REQUIRE(thisWorld.getHostForRank(1) == thisHost);
    REQUIRE(thisWorld.getHostForRank(2) == otherHost);
    REQUIRE(thisWorld.getHostForRank(3) == otherHost);

    thisWorld.destroy();
}

TEST_CASE_METHOD(RemoteMpiTestFixture, "Test send across hosts", "[mpi]")
{
    // Register two ranks (one on each host)
    setWorldSizes(4, 2, 2);
    int rankA = 0;
    int rankB = 2;
    std::vector<int> messageData = { 0, 1, 2 };

    // Init worlds
    MpiWorld& thisWorld = getMpiWorldRegistry().createWorld(msg, worldId);
    faabric::util::setMockMode(false);
    thisWorld.broadcastHostsToRanks();

    // Start the "remote" world in the background
    std::thread otherWorldThread([this, rankA, rankB, &messageData] {
        otherWorld.initialiseFromMsg(msg);

        // Receive the message for the given rank
        MPI_Status status{};
        auto bufferAllocation = std::make_unique<int[]>(messageData.size());
        auto buffer = bufferAllocation.get();
        otherWorld.recv(
          rankA, rankB, BYTES(buffer), MPI_INT, messageData.size(), &status);

        std::vector<int> actual(buffer, buffer + messageData.size());
        assert(actual == messageData);

        assert(status.MPI_SOURCE == rankA);
        assert(status.MPI_ERROR == MPI_SUCCESS);
        assert(status.bytesSize == messageData.size() * sizeof(int));

        otherWorld.destroy();
    });

    // Send a message that should get sent to the "remote" world
    thisWorld.send(
      rankA, rankB, BYTES(messageData.data()), MPI_INT, messageData.size());

    if (otherWorldThread.joinable()) {
        otherWorldThread.join();
    }

    thisWorld.destroy();
}

TEST_CASE_METHOD(RemoteMpiTestFixture,
                 "Test send and recv across hosts",
                 "[mpi]")
{
    // Register two ranks (one on each host)
    setWorldSizes(4, 2, 2);
    int rankA = 0;
    int rankB = 2;
    std::vector<int> messageData = { 0, 1, 2 };
    std::vector<int> messageData2 = { 3, 4, 5 };

    // Init worlds
    MpiWorld& thisWorld = getMpiWorldRegistry().createWorld(msg, worldId);
    faabric::util::setMockMode(false);
    thisWorld.broadcastHostsToRanks();

    std::thread otherWorldThread(
      [this, rankA, rankB, &messageData, &messageData2] {
          otherWorld.initialiseFromMsg(msg);

          // Send a message that should get sent to this host
          otherWorld.send(rankB,
                          rankA,
                          BYTES(messageData.data()),
                          MPI_INT,
                          messageData.size());

          // Now recv
          auto bufferAllocation = std::make_unique<int[]>(messageData2.size());
          auto buffer = bufferAllocation.get();
          otherWorld.recv(rankA,
                          rankB,
                          BYTES(buffer),
                          MPI_INT,
                          messageData2.size(),
                          MPI_STATUS_IGNORE);
          std::vector<int> actual(buffer, buffer + messageData2.size());
          assert(actual == messageData2);

          testLatch->wait();

          otherWorld.destroy();
      });

    // Receive the message for the given rank
    MPI_Status status{};
    auto bufferAllocation = std::make_unique<int[]>(messageData.size());
    auto buffer = bufferAllocation.get();
    thisWorld.recv(
      rankB, rankA, BYTES(buffer), MPI_INT, messageData.size(), &status);
    std::vector<int> actual(buffer, buffer + messageData.size());
    REQUIRE(actual == messageData);

    // Now send a message
    thisWorld.send(
      rankA, rankB, BYTES(messageData2.data()), MPI_INT, messageData2.size());

    REQUIRE(status.MPI_SOURCE == rankB);
    REQUIRE(status.MPI_ERROR == MPI_SUCCESS);
    REQUIRE(status.bytesSize == messageData.size() * sizeof(int));

    testLatch->wait();

    // Clean up
    if (otherWorldThread.joinable()) {
        otherWorldThread.join();
    }

    thisWorld.destroy();
}

TEST_CASE_METHOD(RemoteMpiTestFixture,
                 "Test sending many messages across host",
                 "[mpi]")
{
    // Register two ranks (one on each host)
    setWorldSizes(4, 2, 2);
    int rankA = 0;
    int rankB = 2;
    int numMessages = 1000;

    // Init worlds
    MpiWorld& thisWorld = getMpiWorldRegistry().createWorld(msg, worldId);
    faabric::util::setMockMode(false);

    thisWorld.broadcastHostsToRanks();

    std::thread otherWorldThread([this, rankA, rankB, numMessages] {
        otherWorld.initialiseFromMsg(msg);

        for (int i = 0; i < numMessages; i++) {
            otherWorld.send(rankB, rankA, BYTES(&i), MPI_INT, 1);
        }

        testLatch->wait();
        otherWorld.destroy();
    });

    int recv;
    for (int i = 0; i < numMessages; i++) {
        thisWorld.recv(
          rankB, rankA, BYTES(&recv), MPI_INT, 1, MPI_STATUS_IGNORE);

        // Check in-order delivery
        if (i % (numMessages / 10) == 0) {
            REQUIRE(recv == i);
        }
    }

    // Clean up
    testLatch->wait();
    if (otherWorldThread.joinable()) {
        otherWorldThread.join();
    }

    thisWorld.destroy();
}

TEST_CASE_METHOD(RemoteCollectiveTestFixture,
                 "Test broadcast across hosts",
                 "[mpi]")
{
    MpiWorld& thisWorld = setUpThisWorld();

    std::vector<int> messageData = { 0, 1, 2 };

    std::thread otherWorldThread([this, &messageData] {
        otherWorld.initialiseFromMsg(msg);

        // Broadcast a message from the root first
        otherWorld.broadcast(otherHostRankB,
                             otherHostRankB,
                             BYTES(messageData.data()),
                             MPI_INT,
                             messageData.size(),
                             faabric::MPIMessage::BROADCAST);

        // Check the broadcast is received on this host by the other ranks
        for (int rank : otherWorldRanks) {
            if (rank == otherHostRankB) {
                continue;
            }

            std::vector<int> actual(3, -1);
            otherWorld.broadcast(otherHostRankB,
                                 rank,
                                 BYTES(actual.data()),
                                 MPI_INT,
                                 3,
                                 faabric::MPIMessage::BROADCAST);
            assert(actual == messageData);
        }

        // Give the other host time to receive the broadcast
        testLatch->wait();
        otherWorld.destroy();
    });

    std::vector<int> actual(3, -1);
    // First run the broadcast from the local master (rank = 0)
    thisWorld.broadcast(otherHostRankB,
                        0,
                        BYTES(actual.data()),
                        MPI_INT,
                        3,
                        faabric::MPIMessage::BROADCAST);

    // Check the ranks on this host receive the broadcast
    for (int rank : thisWorldRanks) {
        if (rank == 0) {
            continue;
        }

        thisWorld.broadcast(otherHostRankB,
                            rank,
                            BYTES(actual.data()),
                            MPI_INT,
                            3,
                            faabric::MPIMessage::BROADCAST);
        REQUIRE(actual == messageData);
    }

    // Clean up
    testLatch->wait();
    if (otherWorldThread.joinable()) {
        otherWorldThread.join();
    }

    thisWorld.destroy();
}

TEST_CASE_METHOD(RemoteCollectiveTestFixture,
                 "Test scatter across hosts",
                 "[mpi]")
{
    MpiWorld& thisWorld = setUpThisWorld();

    // Build the data
    int nPerRank = 4;
    int dataSize = nPerRank * thisWorldSize;
    std::vector<int> messageData(dataSize, 0);
    for (int i = 0; i < dataSize; i++) {
        messageData[i] = i;
    }

    std::thread otherWorldThread([this, nPerRank, &messageData] {
        otherWorld.initialiseFromMsg(msg);

        // Do the scatter (when send rank == recv rank)
        std::vector<int> actual(nPerRank, -1);
        otherWorld.scatter(otherHostRankB,
                           otherHostRankB,
                           BYTES(messageData.data()),
                           MPI_INT,
                           nPerRank,
                           BYTES(actual.data()),
                           MPI_INT,
                           nPerRank);

        // Build the expectation
        std::vector<std::vector<int>> expected(otherWorldRanks.size(),
                                               std::vector<int>(nPerRank));
        for (int i = 0; i < otherWorldRanks.size(); i++) {
            for (int j = 0; j < nPerRank; j++) {
                expected.at(i).at(j) = otherWorldRanks.at(i) * nPerRank + j;
            }
        }

        // Check for root
        assert(actual == expected.at(0));

        // Check the other ranks on this host have received the data
        otherWorld.scatter(otherHostRankB,
                           otherHostRankA,
                           nullptr,
                           MPI_INT,
                           nPerRank,
                           BYTES(actual.data()),
                           MPI_INT,
                           nPerRank);
        assert(actual == expected.at(2));

        otherWorld.scatter(otherHostRankB,
                           otherHostRankC,
                           nullptr,
                           MPI_INT,
                           nPerRank,
                           BYTES(actual.data()),
                           MPI_INT,
                           nPerRank);
        assert(actual == expected.at(1));

        testLatch->wait();
        otherWorld.destroy();
    });

    // Build the expectation
    std::vector<std::vector<int>> expected(thisWorldRanks.size(),
                                           std::vector<int>(nPerRank));
    for (int i = 0; i < thisWorldRanks.size(); i++) {
        for (int j = 0; j < nPerRank; j++) {
            expected.at(i).at(j) = thisWorldRanks.at(i) * nPerRank + j;
        }
    }

    // Check for ranks on this host
    std::vector<int> actual(nPerRank, -1);
    thisWorld.scatter(otherHostRankB,
                      0,
                      nullptr,
                      MPI_INT,
                      nPerRank,
                      BYTES(actual.data()),
                      MPI_INT,
                      nPerRank);
    REQUIRE(actual == expected.at(2));

    thisWorld.scatter(otherHostRankB,
                      thisHostRankB,
                      nullptr,
                      MPI_INT,
                      nPerRank,
                      BYTES(actual.data()),
                      MPI_INT,
                      nPerRank);
    REQUIRE(actual == expected.at(0));

    thisWorld.scatter(otherHostRankB,
                      thisHostRankA,
                      nullptr,
                      MPI_INT,
                      nPerRank,
                      BYTES(actual.data()),
                      MPI_INT,
                      nPerRank);
    REQUIRE(actual == expected.at(1));

    // Clean up
    testLatch->wait();
    if (otherWorldThread.joinable()) {
        otherWorldThread.join();
    }

    thisWorld.destroy();
}

TEST_CASE_METHOD(RemoteCollectiveTestFixture,
                 "Test gather across hosts",
                 "[mpi]")
{
    MpiWorld& thisWorld = setUpThisWorld();

    // Build the data for each rank
    int nPerRank = 4;
    std::vector<std::vector<int>> rankData;
    for (int i = 0; i < thisWorldSize; i++) {
        std::vector<int> thisRankData;
        for (int j = 0; j < nPerRank; j++) {
            thisRankData.push_back((i * nPerRank) + j);
        }

        rankData.push_back(thisRankData);
    }

    // Build the expectation
    std::vector<int> expected;
    for (int i = 0; i < thisWorldSize * nPerRank; i++) {
        expected.push_back(i);
    }

    std::vector<int> actual(thisWorldSize * nPerRank, -1);

    // Call gather for each rank other than the root (out of order)
    int root = thisHostRankA;
    std::thread otherWorldThread([this, root, &rankData, nPerRank] {
        otherWorld.initialiseFromMsg(msg);

        for (int rank : otherWorldRanks) {
            otherWorld.gather(rank,
                              root,
                              BYTES(rankData[rank].data()),
                              MPI_INT,
                              nPerRank,
                              nullptr,
                              MPI_INT,
                              nPerRank);
        }

        testLatch->wait();
        otherWorld.destroy();
    });

    for (int rank : thisWorldRanks) {
        if (rank == root) {
            continue;
        }
        thisWorld.gather(rank,
                         root,
                         BYTES(rankData[rank].data()),
                         MPI_INT,
                         nPerRank,
                         nullptr,
                         MPI_INT,
                         nPerRank);
    }

    // Call gather for root
    thisWorld.gather(root,
                     root,
                     BYTES(rankData[root].data()),
                     MPI_INT,
                     nPerRank,
                     BYTES(actual.data()),
                     MPI_INT,
                     nPerRank);

    // Check data
    REQUIRE(actual == expected);

    // Clean up
    testLatch->wait();
    if (otherWorldThread.joinable()) {
        otherWorldThread.join();
    }

    thisWorld.destroy();
}

TEST_CASE_METHOD(RemoteMpiTestFixture,
                 "Test sending sync and async message to same host",
                 "[mpi]")
{
    // Allocate two ranks in total, one rank per host
    setWorldSizes(4, 2, 2);
    int sendRank = 2;
    int recvRank = 0;
    std::vector<int> messageData = { 0, 1, 2 };

    // Init world
    MpiWorld& thisWorld = getMpiWorldRegistry().createWorld(msg, worldId);
    faabric::util::setMockMode(false);
    thisWorld.broadcastHostsToRanks();

    std::thread otherWorldThread([this, sendRank, recvRank, &messageData] {
        otherWorld.initialiseFromMsg(msg);

        // Send message twice
        otherWorld.send(sendRank,
                        recvRank,
                        BYTES(messageData.data()),
                        MPI_INT,
                        messageData.size());
        otherWorld.send(sendRank,
                        recvRank,
                        BYTES(messageData.data()),
                        MPI_INT,
                        messageData.size());

        testLatch->wait();
        otherWorld.destroy();
    });

    // Receive one message asynchronously
    std::vector<int> asyncMessage(messageData.size(), 0);
    int recvId = thisWorld.irecv(sendRank,
                                 recvRank,
                                 BYTES(asyncMessage.data()),
                                 MPI_INT,
                                 asyncMessage.size());

    // Receive one message synchronously
    std::vector<int> syncMessage(messageData.size(), 0);
    thisWorld.recv(sendRank,
                   recvRank,
                   BYTES(syncMessage.data()),
                   MPI_INT,
                   syncMessage.size(),
                   MPI_STATUS_IGNORE);

    // Wait for the async message
    thisWorld.awaitAsyncRequest(recvId);

    // Checks
    REQUIRE(syncMessage == messageData);
    REQUIRE(asyncMessage == messageData);

    // Clean up
    testLatch->wait();
    if (otherWorldThread.joinable()) {
        otherWorldThread.join();
    }

    thisWorld.destroy();
}

TEST_CASE_METHOD(RemoteMpiTestFixture,
                 "Test receiving remote async requests out of order",
                 "[mpi]")
{
    // Allocate two ranks in total, one rank per host
    setWorldSizes(4, 2, 2);
    int sendRank = 2;
    int recvRank = 0;

    // Init world
    MpiWorld& thisWorld = getMpiWorldRegistry().createWorld(msg, worldId);
    faabric::util::setMockMode(false);
    thisWorld.broadcastHostsToRanks();

    std::thread otherWorldThread([this, sendRank, recvRank] {
        otherWorld.initialiseFromMsg(msg);

        // Send different messages
        for (int i = 0; i < 3; i++) {
            otherWorld.send(sendRank, recvRank, BYTES(&i), MPI_INT, 1);
        }

        testLatch->wait();
        otherWorld.destroy();
    });

    // Receive two messages asynchronously
    int recv1, recv2, recv3;
    int recvId1 =
      thisWorld.irecv(sendRank, recvRank, BYTES(&recv1), MPI_INT, 1);

    int recvId2 =
      thisWorld.irecv(sendRank, recvRank, BYTES(&recv2), MPI_INT, 1);

    // Receive one message synchronously
    thisWorld.recv(
      sendRank, recvRank, BYTES(&recv3), MPI_INT, 1, MPI_STATUS_IGNORE);

    SECTION("Wait out of order")
    {
        thisWorld.awaitAsyncRequest(recvId2);
        thisWorld.awaitAsyncRequest(recvId1);
    }

    SECTION("Wait in order")
    {
        thisWorld.awaitAsyncRequest(recvId1);
        thisWorld.awaitAsyncRequest(recvId2);
    }

    // Checks
    REQUIRE(recv1 == 0);
    REQUIRE(recv2 == 1);
    REQUIRE(recv3 == 2);

    // Clean up
    testLatch->wait();
    if (otherWorldThread.joinable()) {
        otherWorldThread.join();
    }

    thisWorld.destroy();
}

TEST_CASE_METHOD(RemoteMpiTestFixture,
                 "Test ring sendrecv across hosts",
                 "[mpi]")
{
    // Allocate two ranks in total, one rank per host
    setWorldSizes(3, 1, 2);
    int worldSize = 3;
    std::vector<int> thisHostRanks = { 0 };

    // Init world
    MpiWorld& thisWorld = getMpiWorldRegistry().createWorld(msg, worldId);
    faabric::util::setMockMode(false);
    thisWorld.broadcastHostsToRanks();

    std::thread otherWorldThread([this, worldSize] {
        std::vector<int> otherHostRanks = { 1, 2 };
        otherWorld.initialiseFromMsg(msg);

        // Send different messages
        for (auto& rank : otherHostRanks) {
            int left = rank > 0 ? rank - 1 : worldSize - 1;
            int right = (rank + 1) % worldSize;
            int recvData = -1;

            otherWorld.sendRecv(BYTES(&rank),
                                1,
                                MPI_INT,
                                right,
                                BYTES(&recvData),
                                1,
                                MPI_INT,
                                left,
                                rank,
                                MPI_STATUS_IGNORE);
        }

        testLatch->wait();
        otherWorld.destroy();
    });

    for (auto& rank : thisHostRanks) {
        int left = rank > 0 ? rank - 1 : worldSize - 1;
        int right = (rank + 1) % worldSize;
        int recvData = -1;

        thisWorld.sendRecv(BYTES(&rank),
                           1,
                           MPI_INT,
                           right,
                           BYTES(&recvData),
                           1,
                           MPI_INT,
                           left,
                           rank,
                           MPI_STATUS_IGNORE);

        REQUIRE(recvData == left);
    }

    // Clean up
    testLatch->wait();
    if (otherWorldThread.joinable()) {
        otherWorldThread.join();
    }

    thisWorld.destroy();
}

TEST_CASE_METHOD(RemoteMpiTestFixture,
                 "Test remote message endpoint creation",
                 "[mpi]")
{
    // Register two ranks (one on each host)
    setWorldSizes(4, 2, 2);
    int rankA = 0;
    int rankB = 2;
    std::vector<int> messageData = { 0, 1, 2 };
    std::vector<int> messageData2 = { 3, 4 };

    // Init worlds
    MpiWorld& thisWorld = getMpiWorldRegistry().createWorld(msg, worldId);
    faabric::util::setMockMode(false);
    thisWorld.broadcastHostsToRanks();

    std::thread otherWorldThread(
      [this, rankA, rankB, &messageData, &messageData2] {
          otherWorld.initialiseFromMsg(msg);

          // Recv once
          auto bufferAllocation = std::make_unique<int[]>(messageData.size());
          auto buffer = bufferAllocation.get();
          otherWorld.recv(rankA,
                          rankB,
                          BYTES(buffer),
                          MPI_INT,
                          messageData.size(),
                          MPI_STATUS_IGNORE);
          std::vector<int> actual(buffer, buffer + messageData.size());
          assert(actual == messageData);

          // Recv a second time
          auto buffer2Allocation = std::make_unique<int[]>(messageData2.size());
          auto buffer2 = buffer2Allocation.get();
          otherWorld.recv(rankA,
                          rankB,
                          BYTES(buffer2),
                          MPI_INT,
                          messageData2.size(),
                          MPI_STATUS_IGNORE);
          std::vector<int> actual2(buffer2, buffer2 + messageData2.size());
          assert(actual2 == messageData2);

          // Send last message
          otherWorld.send(rankB,
                          rankA,
                          BYTES(messageData.data()),
                          MPI_INT,
                          messageData.size());

          testLatch->wait();

          otherWorld.destroy();
      });

    std::vector<bool> endpointCheck;
    std::vector<bool> expectedEndpoints = { false, false, true,  false,
                                            false, false, false, false,
                                            false, false, false, false,
                                            false, false, false, false };

    // Sending a message initialises the remote endpoint
    thisWorld.send(
      rankA, rankB, BYTES(messageData.data()), MPI_INT, messageData.size());

    // Check the right messaging endpoint has been created
    endpointCheck = thisWorld.getInitedRemoteMpiEndpoints();
    REQUIRE(endpointCheck == expectedEndpoints);

    // Sending a second message re-uses the existing endpoint
    thisWorld.send(
      rankA, rankB, BYTES(messageData2.data()), MPI_INT, messageData2.size());

    // Check that no additional endpoints have been created
    endpointCheck = thisWorld.getInitedRemoteMpiEndpoints();
    REQUIRE(endpointCheck == expectedEndpoints);

    // Finally recv a messge, the same endpoint should be used again
    auto bufferAllocation = std::make_unique<int[]>(messageData.size());
    auto buffer = bufferAllocation.get();
    thisWorld.recv(rankB,
                   rankA,
                   BYTES(buffer),
                   MPI_INT,
                   messageData.size(),
                   MPI_STATUS_IGNORE);
    std::vector<int> actual(buffer, buffer + messageData.size());
    assert(actual == messageData);

    // Check that no extra endpoint has been created
    endpointCheck = thisWorld.getInitedRemoteMpiEndpoints();
    REQUIRE(endpointCheck == expectedEndpoints);

    testLatch->wait();

    // Clean up
    if (otherWorldThread.joinable()) {
        otherWorldThread.join();
    }

    thisWorld.destroy();
}

TEST_CASE_METHOD(RemoteMpiTestFixture, "Test UMB creation", "[mpi]")
{
    // Register three ranks
    setWorldSizes(3, 1, 2);
    int thisWorldRank = 0;
    int otherWorldRank1 = 1;
    int otherWorldRank2 = 2;
    std::vector<int> messageData = { 0, 1, 2 };
    std::vector<int> messageData2 = { 3, 4 };

    // Init worlds
    MpiWorld& thisWorld = getMpiWorldRegistry().createWorld(msg, worldId);
    faabric::util::setMockMode(false);
    thisWorld.broadcastHostsToRanks();

    std::thread otherWorldThread([this,
                                  thisWorldRank,
                                  otherWorldRank1,
                                  otherWorldRank2,
                                  &messageData,
                                  &messageData2] {
        otherWorld.initialiseFromMsg(msg);

        // Send message from one rank
        otherWorld.send(otherWorldRank1,
                        thisWorldRank,
                        BYTES(messageData.data()),
                        MPI_INT,
                        messageData.size());

        // Send message from one rank
        otherWorld.send(otherWorldRank2,
                        thisWorldRank,
                        BYTES(messageData2.data()),
                        MPI_INT,
                        messageData2.size());

        testLatch->wait();

        otherWorld.destroy();
    });

    std::vector<bool> umbCheck;
    std::vector<bool> expectedUmb1 = { false, false, false, true, false,
                                       false, false, false, false };
    std::vector<bool> expectedUmb2 = { false, false, false, true, false,
                                       false, true,  false, false };

    // Irecv a messge from one rank, another UMB should be created
    auto buffer1Allocation = std::make_unique<int[]>(messageData.size());
    auto buffer1 = buffer1Allocation.get();
    int recvId1 = thisWorld.irecv(otherWorldRank1,
                                  thisWorldRank,
                                  BYTES(buffer1),
                                  MPI_INT,
                                  messageData.size());

    // Check that an endpoint has been created
    umbCheck = thisWorld.getInitedUMB();
    REQUIRE(umbCheck == expectedUmb1);

    // Irecv a messge from another rank, another UMB should be created
    auto buffer2Allocation = std::make_unique<int[]>(messageData.size());
    auto buffer2 = buffer2Allocation.get();
    int recvId2 = thisWorld.irecv(otherWorldRank2,
                                  thisWorldRank,
                                  BYTES(buffer2),
                                  MPI_INT,
                                  messageData2.size());

    // Check that an extra endpoint has been created
    umbCheck = thisWorld.getInitedUMB();
    REQUIRE(umbCheck == expectedUmb2);

    // Wait for both messages
    thisWorld.awaitAsyncRequest(recvId1);
    thisWorld.awaitAsyncRequest(recvId2);

    // Sanity check the message content
    std::vector<int> actual1(buffer1, buffer1 + messageData.size());
    assert(actual1 == messageData);
    std::vector<int> actual2(buffer2, buffer2 + messageData2.size());
    assert(actual2 == messageData2);

    testLatch->wait();

    // Clean up
    if (otherWorldThread.joinable()) {
        otherWorldThread.join();
    }

    thisWorld.destroy();
}

std::set<int> getReceiversFromMessages(
  std::vector<std::shared_ptr<faabric::MPIMessage>> msgs)
{
    std::set<int> receivers;
    for (const auto& msg : msgs) {
        receivers.insert(msg->destination());
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
    faabric::util::setMockMode(true);
    thisWorld.broadcastHostsToRanks();
    REQUIRE(getMpiHostsToRanksMessages().size() == 1);
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
                            faabric::MPIMessage::BROADCAST);
    } else {
        otherWorld.broadcast(sendRank,
                             recvRank,
                             BYTES(messageData.data()),
                             MPI_INT,
                             messageData.size(),
                             faabric::MPIMessage::BROADCAST);
    }
    auto msgs = getMpiMockedMessages(recvRank);
    REQUIRE(msgs.size() == expectedNumMsg);
    REQUIRE(getReceiversFromMessages(msgs) == expectedRecvRanks);

    faabric::util::setMockMode(false);
    otherWorld.destroy();
    thisWorld.destroy();
}
}
