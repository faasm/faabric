#include <catch.hpp>

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
      : thisWorldSize(6)
      , remoteRankA(1)
      , remoteRankB(2)
      , remoteRankC(3)
      , localRankA(4)
      , localRankB(5)
      , remoteWorldRanks({ remoteRankB, remoteRankC, remoteRankA })
      , localWorldRanks({ localRankB, localRankA, 0 })
    {}

  protected:
    int thisWorldSize;
    int remoteRankA, remoteRankB, remoteRankC;
    int localRankA, localRankB;
    std::vector<int> remoteWorldRanks;
    std::vector<int> localWorldRanks;
};

TEST_CASE_METHOD(RemoteMpiTestFixture, "Test rank allocation", "[mpi]")
{
    // Allocate two ranks in total, one rank per host
    this->setWorldsSizes(2, 1, 1);

    // Init worlds
    MpiWorld& localWorld = getMpiWorldRegistry().createWorld(msg, worldId);
    remoteWorld.initialiseFromMsg(msg);
    faabric::util::setMockMode(false);

    // Now check both world instances report the same mappings
    REQUIRE(localWorld.getHostForRank(0) == thisHost);
    REQUIRE(localWorld.getHostForRank(1) == otherHost);

    // Destroy worlds
    localWorld.destroy();
    remoteWorld.destroy();
}

TEST_CASE_METHOD(RemoteMpiTestFixture, "Test send across hosts", "[mpi]")
{
    // Register two ranks (one on each host)
    this->setWorldsSizes(2, 1, 1);
    int rankA = 0;
    int rankB = 1;
    std::vector<int> messageData = { 0, 1, 2 };

    // Init worlds
    MpiWorld& localWorld = getMpiWorldRegistry().createWorld(msg, worldId);
    faabric::util::setMockMode(false);

    std::thread senderThread([this, rankA, rankB] {
        std::vector<int> messageData = { 0, 1, 2 };

        remoteWorld.initialiseFromMsg(msg);

        // Send a message that should get sent to this host
        remoteWorld.send(
          rankB, rankA, BYTES(messageData.data()), MPI_INT, messageData.size());
        remoteWorld.destroy();
    });

    SECTION("Check recv")
    {
        // Receive the message for the given rank
        MPI_Status status{};
        auto buffer = new int[messageData.size()];
        localWorld.recv(
          rankB, rankA, BYTES(buffer), MPI_INT, messageData.size(), &status);

        std::vector<int> actual(buffer, buffer + messageData.size());
        REQUIRE(actual == messageData);

        REQUIRE(status.MPI_SOURCE == rankB);
        REQUIRE(status.MPI_ERROR == MPI_SUCCESS);
        REQUIRE(status.bytesSize == messageData.size() * sizeof(int));
    }

    // Destroy worlds
    senderThread.join();
    localWorld.destroy();
}

TEST_CASE_METHOD(RemoteMpiTestFixture,
                 "Test sending many messages across host",
                 "[mpi]")
{
    // Register two ranks (one on each host)
    this->setWorldsSizes(2, 1, 1);
    int rankA = 0;
    int rankB = 1;
    int numMessages = 1000;

    // Init worlds
    MpiWorld& localWorld = getMpiWorldRegistry().createWorld(msg, worldId);
    faabric::util::setMockMode(false);

    std::thread senderThread([this, rankA, rankB, numMessages] {
        remoteWorld.initialiseFromMsg(msg);

        for (int i = 0; i < numMessages; i++) {
            remoteWorld.send(rankB, rankA, BYTES(&i), MPI_INT, 1);
        }
        remoteWorld.destroy();
    });

    int recv;
    for (int i = 0; i < numMessages; i++) {
        localWorld.recv(
          rankB, rankA, BYTES(&recv), MPI_INT, 1, MPI_STATUS_IGNORE);

        // Check in-order delivery
        if (i % (numMessages / 10) == 0) {
            REQUIRE(recv == i);
        }
    }

    // Destroy worlds
    senderThread.join();
    localWorld.destroy();
}

TEST_CASE_METHOD(RemoteCollectiveTestFixture,
                 "Test broadcast across hosts",
                 "[mpi]")
{
    // Here we rely on the scheduler running out of resources, and overloading
    // the localWorld with ranks 4 and 5
    this->setWorldsSizes(thisWorldSize, 1, 3);
    std::vector<int> messageData = { 0, 1, 2 };

    // Init worlds
    MpiWorld& localWorld = getMpiWorldRegistry().createWorld(msg, worldId);
    faabric::util::setMockMode(false);

    std::thread senderThread([this, &messageData] {
        remoteWorld.initialiseFromMsg(msg);

        // Broadcast a message
        remoteWorld.broadcast(
          remoteRankB, BYTES(messageData.data()), MPI_INT, messageData.size());

        // Check the host that the root is on
        for (int rank : remoteWorldRanks) {
            if (rank == remoteRankB) {
                continue;
            }

            std::vector<int> actual(3, -1);
            remoteWorld.recv(
              remoteRankB, rank, BYTES(actual.data()), MPI_INT, 3, nullptr);
            assert(actual == messageData);
        }

        remoteWorld.destroy();
    });

    // Check the local host
    for (int rank : localWorldRanks) {
        std::vector<int> actual(3, -1);
        localWorld.recv(
          remoteRankB, rank, BYTES(actual.data()), MPI_INT, 3, nullptr);
        REQUIRE(actual == messageData);
    }

    senderThread.join();
    localWorld.destroy();
}

TEST_CASE_METHOD(RemoteCollectiveTestFixture,
                 "Test scatter across hosts",
                 "[mpi]")
{
    // Here we rely on the scheduler running out of resources, and overloading
    // the localWorld with ranks 4 and 5
    this->setWorldsSizes(thisWorldSize, 1, 3);

    // Init worlds
    MpiWorld& localWorld = getMpiWorldRegistry().createWorld(msg, worldId);
    faabric::util::setMockMode(false);

    // Build the data
    int nPerRank = 4;
    int dataSize = nPerRank * thisWorldSize;
    std::vector<int> messageData(dataSize, 0);
    for (int i = 0; i < dataSize; i++) {
        messageData[i] = i;
    }

    std::thread senderThread([this, nPerRank, &messageData] {
        remoteWorld.initialiseFromMsg(msg);
        // Do the scatter
        std::vector<int> actual(nPerRank, -1);
        remoteWorld.scatter(remoteRankB,
                            remoteRankB,
                            BYTES(messageData.data()),
                            MPI_INT,
                            nPerRank,
                            BYTES(actual.data()),
                            MPI_INT,
                            nPerRank);

        // Check for root
        assert(actual == std::vector<int>({ 8, 9, 10, 11 }));

        // Check for other remote ranks
        remoteWorld.scatter(remoteRankB,
                            remoteRankA,
                            nullptr,
                            MPI_INT,
                            nPerRank,
                            BYTES(actual.data()),
                            MPI_INT,
                            nPerRank);
        assert(actual == std::vector<int>({ 4, 5, 6, 7 }));

        remoteWorld.scatter(remoteRankB,
                            remoteRankC,
                            nullptr,
                            MPI_INT,
                            nPerRank,
                            BYTES(actual.data()),
                            MPI_INT,
                            nPerRank);
        assert(actual == std::vector<int>({ 12, 13, 14, 15 }));

        remoteWorld.destroy();
    });

    // Check for local ranks
    std::vector<int> actual(nPerRank, -1);
    localWorld.scatter(remoteRankB,
                       0,
                       nullptr,
                       MPI_INT,
                       nPerRank,
                       BYTES(actual.data()),
                       MPI_INT,
                       nPerRank);
    REQUIRE(actual == std::vector<int>({ 0, 1, 2, 3 }));

    localWorld.scatter(remoteRankB,
                       localRankB,
                       nullptr,
                       MPI_INT,
                       nPerRank,
                       BYTES(actual.data()),
                       MPI_INT,
                       nPerRank);
    REQUIRE(actual == std::vector<int>({ 20, 21, 22, 23 }));

    localWorld.scatter(remoteRankB,
                       localRankA,
                       nullptr,
                       MPI_INT,
                       nPerRank,
                       BYTES(actual.data()),
                       MPI_INT,
                       nPerRank);
    REQUIRE(actual == std::vector<int>({ 16, 17, 18, 19 }));

    senderThread.join();
    localWorld.destroy();
}

TEST_CASE_METHOD(RemoteCollectiveTestFixture,
                 "Test gather across hosts",
                 "[mpi]")
{
    // Here we rely on the scheduler running out of resources, and overloading
    // the localWorld with ranks 4 and 5
    this->setWorldsSizes(thisWorldSize, 1, 3);

    // Init worlds
    MpiWorld& localWorld = getMpiWorldRegistry().createWorld(msg, worldId);
    faabric::util::setMockMode(false);

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
    int root = localRankA;
    std::thread senderThread([this, root, &rankData, nPerRank] {
        remoteWorld.initialiseFromMsg(msg);

        for (int rank : remoteWorldRanks) {
            remoteWorld.gather(rank,
                               root,
                               BYTES(rankData[rank].data()),
                               MPI_INT,
                               nPerRank,
                               nullptr,
                               MPI_INT,
                               nPerRank);
        }

        remoteWorld.destroy();
    });

    for (int rank : localWorldRanks) {
        if (rank == root) {
            continue;
        }
        localWorld.gather(rank,
                          root,
                          BYTES(rankData[rank].data()),
                          MPI_INT,
                          nPerRank,
                          nullptr,
                          MPI_INT,
                          nPerRank);
    }

    // Call gather for root
    localWorld.gather(root,
                      root,
                      BYTES(rankData[root].data()),
                      MPI_INT,
                      nPerRank,
                      BYTES(actual.data()),
                      MPI_INT,
                      nPerRank);

    // Check data
    REQUIRE(actual == expected);

    senderThread.join();
    localWorld.destroy();
}

TEST_CASE_METHOD(RemoteMpiTestFixture,
                 "Test sending sync and async message to same host",
                 "[mpi]")
{
    // Allocate two ranks in total, one rank per host
    this->setWorldsSizes(2, 1, 1);
    int sendRank = 1;
    int recvRank = 0;
    std::vector<int> messageData = { 0, 1, 2 };

    // Init world
    MpiWorld& localWorld = getMpiWorldRegistry().createWorld(msg, worldId);
    faabric::util::setMockMode(false);

    std::thread senderThread([this, sendRank, recvRank] {
        std::vector<int> messageData = { 0, 1, 2 };

        remoteWorld.initialiseFromMsg(msg);

        // Send message twice
        remoteWorld.send(sendRank,
                         recvRank,
                         BYTES(messageData.data()),
                         MPI_INT,
                         messageData.size());
        remoteWorld.send(sendRank,
                         recvRank,
                         BYTES(messageData.data()),
                         MPI_INT,
                         messageData.size());

        remoteWorld.destroy();
    });

    // Receive one message asynchronously
    std::vector<int> asyncMessage(messageData.size(), 0);
    int recvId = localWorld.irecv(sendRank,
                                  recvRank,
                                  BYTES(asyncMessage.data()),
                                  MPI_INT,
                                  asyncMessage.size());

    // Receive one message synchronously
    std::vector<int> syncMessage(messageData.size(), 0);
    localWorld.recv(sendRank,
                    recvRank,
                    BYTES(syncMessage.data()),
                    MPI_INT,
                    syncMessage.size(),
                    MPI_STATUS_IGNORE);

    // Wait for the async message
    localWorld.awaitAsyncRequest(recvId);

    // Checks
    REQUIRE(syncMessage == messageData);
    REQUIRE(asyncMessage == messageData);

    // Destroy world
    senderThread.join();
    localWorld.destroy();
}

TEST_CASE_METHOD(RemoteMpiTestFixture,
                 "Test receiving remote async requests out of order",
                 "[mpi]")
{
    // Allocate two ranks in total, one rank per host
    this->setWorldsSizes(2, 1, 1);
    int sendRank = 1;
    int recvRank = 0;

    // Init world
    MpiWorld& localWorld = getMpiWorldRegistry().createWorld(msg, worldId);
    faabric::util::setMockMode(false);

    std::thread senderThread([this, sendRank, recvRank] {
        remoteWorld.initialiseFromMsg(msg);

        // Send different messages
        for (int i = 0; i < 3; i++) {
            remoteWorld.send(sendRank, recvRank, BYTES(&i), MPI_INT, 1);
        }

        remoteWorld.destroy();
    });

    // Receive two messages asynchronously
    int recv1, recv2, recv3;
    int recvId1 =
      localWorld.irecv(sendRank, recvRank, BYTES(&recv1), MPI_INT, 1);

    int recvId2 =
      localWorld.irecv(sendRank, recvRank, BYTES(&recv2), MPI_INT, 1);

    // Receive one message synchronously
    localWorld.recv(
      sendRank, recvRank, BYTES(&recv3), MPI_INT, 1, MPI_STATUS_IGNORE);

    SECTION("Wait out of order")
    {
        localWorld.awaitAsyncRequest(recvId2);
        localWorld.awaitAsyncRequest(recvId1);
    }

    SECTION("Wait in order")
    {
        localWorld.awaitAsyncRequest(recvId1);
        localWorld.awaitAsyncRequest(recvId2);
    }

    // Checks
    REQUIRE(recv1 == 0);
    REQUIRE(recv2 == 1);
    REQUIRE(recv3 == 2);

    // Destroy world
    senderThread.join();
    localWorld.destroy();
}

TEST_CASE_METHOD(RemoteMpiTestFixture,
                 "Test ring sendrecv across hosts",
                 "[mpi]")
{
    // Allocate two ranks in total, one rank per host
    this->setWorldsSizes(3, 1, 2);
    int worldSize = 3;
    std::vector<int> localRanks = { 0 };

    // Init world
    MpiWorld& localWorld = getMpiWorldRegistry().createWorld(msg, worldId);
    faabric::util::setMockMode(false);

    std::thread senderThread([this, worldSize] {
        std::vector<int> remoteRanks = { 1, 2 };
        remoteWorld.initialiseFromMsg(msg);

        // Send different messages
        for (auto& rank : remoteRanks) {
            int left = rank > 0 ? rank - 1 : worldSize - 1;
            int right = (rank + 1) % worldSize;
            int recvData = -1;

            remoteWorld.sendRecv(BYTES(&rank),
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

        remoteWorld.destroy();
    });

    for (auto& rank : localRanks) {
        int left = rank > 0 ? rank - 1 : worldSize - 1;
        int right = (rank + 1) % worldSize;
        int recvData = -1;

        localWorld.sendRecv(BYTES(&rank),
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

    // Destroy world
    senderThread.join();
    localWorld.destroy();
}
}
