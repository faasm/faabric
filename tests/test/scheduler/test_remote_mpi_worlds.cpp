#include <catch.hpp>

#include <faabric/mpi/mpi.h>
#include <faabric/scheduler/FunctionCallServer.h>
#include <faabric/scheduler/MpiWorldRegistry.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/bytes.h>
#include <faabric/util/macros.h>
#include <faabric_utils.h>

using namespace faabric::scheduler;

namespace tests {
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
    // Start a server on this host
    FunctionCallServer server;
    server.start();
    usleep(1000 * 100);

    // Register two ranks (one on each host)
    this->setWorldsSizes(2, 1, 1);
    int rankA = 0;
    int rankB = 1;
    std::vector<int> messageData = { 0, 1, 2 };

    // Init worlds
    MpiWorld& localWorld = getMpiWorldRegistry().createWorld(msg, worldId);
    remoteWorld.initialiseFromMsg(msg);
    faabric::util::setMockMode(false);

    // Send a message that should get sent to this host
    remoteWorld.send(
      rankB, rankA, BYTES(messageData.data()), MPI_INT, messageData.size());
    usleep(1000 * 100);

    SECTION("Check queueing")
    {
        REQUIRE(localWorld.getLocalQueueSize(rankB, rankA) == 1);

        // Check message content
        faabric::MPIMessage actualMessage =
          *(localWorld.getLocalQueue(rankB, rankA)->dequeue());
        REQUIRE(actualMessage.worldid() == worldId);
        REQUIRE(actualMessage.count() == messageData.size());
        REQUIRE(actualMessage.sender() == rankB);
        REQUIRE(actualMessage.destination() == rankA);
    }

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
    localWorld.destroy();
    remoteWorld.destroy();

    server.stop();
}

TEST_CASE_METHOD(RemoteMpiTestFixture,
                 "Test collective messaging across hosts",
                 "[mpi]")
{
    FunctionCallServer server;
    server.start();
    usleep(1000 * 100);

    // Here we rely on the scheduler running out of resources, and overloading
    // the localWorld with ranks 4 and 5
    int thisWorldSize = 6;
    this->setWorldsSizes(thisWorldSize, 1, 3);
    int remoteRankA = 1;
    int remoteRankB = 2;
    int remoteRankC = 3;
    int localRankA = 4;
    int localRankB = 5;

    // Init worlds
    MpiWorld& localWorld = getMpiWorldRegistry().createWorld(msg, worldId);
    remoteWorld.initialiseFromMsg(msg);
    faabric::util::setMockMode(false);

    // Note that ranks are deliberately out of order
    std::vector<int> remoteWorldRanks = { remoteRankB,
                                          remoteRankC,
                                          remoteRankA };
    std::vector<int> localWorldRanks = { localRankB, localRankA, 0 };

    SECTION("Broadcast")
    {
        // Broadcast a message
        std::vector<int> messageData = { 0, 1, 2 };
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
            REQUIRE(actual == messageData);
        }

        // Check the local host
        for (int rank : localWorldRanks) {
            std::vector<int> actual(3, -1);
            localWorld.recv(
              remoteRankB, rank, BYTES(actual.data()), MPI_INT, 3, nullptr);
            REQUIRE(actual == messageData);
        }
    }

    SECTION("Scatter")
    {
        // Build the data
        int nPerRank = 4;
        int dataSize = nPerRank * thisWorldSize;
        std::vector<int> messageData(dataSize, 0);
        for (int i = 0; i < dataSize; i++) {
            messageData[i] = i;
        }

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
        REQUIRE(actual == std::vector<int>({ 8, 9, 10, 11 }));

        // Check for other remote ranks
        remoteWorld.scatter(remoteRankB,
                            remoteRankA,
                            nullptr,
                            MPI_INT,
                            nPerRank,
                            BYTES(actual.data()),
                            MPI_INT,
                            nPerRank);
        REQUIRE(actual == std::vector<int>({ 4, 5, 6, 7 }));

        remoteWorld.scatter(remoteRankB,
                            remoteRankC,
                            nullptr,
                            MPI_INT,
                            nPerRank,
                            BYTES(actual.data()),
                            MPI_INT,
                            nPerRank);
        REQUIRE(actual == std::vector<int>({ 12, 13, 14, 15 }));

        // Check for local ranks
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
    }

    SECTION("Gather and allgather")
    {
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

        SECTION("Gather")
        {
            std::vector<int> actual(thisWorldSize * nPerRank, -1);

            // Call gather for each rank other than the root (out of order)
            int root = localRankA;
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
        }
    }

    // Destroy worlds
    localWorld.destroy();
    remoteWorld.destroy();

    server.stop();
}
}
