#include <catch2/catch.hpp>

#include <faabric_utils.h>

using namespace faabric::scheduler;

namespace tests {
class MultiWorldMpiTestFixture : public MpiBaseTestFixture
{
  public:
    MultiWorldMpiTestFixture()
    {
        faabric::Message msgA = faabric::util::messageFactory(userA, funcA);
        faabric::Message msgB = faabric::util::messageFactory(userB, funcB);
        msgA.set_mpiworldsize(worldSizeA);
        msgA.set_mpiworldid(worldIdA);
        msgB.set_mpiworldsize(worldSizeB);
        msgB.set_mpiworldid(worldIdB);

        worldA.create(msgA, worldIdA, worldSizeA);
        worldB.create(msgB, worldIdB, worldSizeB);
    }

    ~MultiWorldMpiTestFixture()
    {
        worldA.destroy();
        worldB.destroy();
    }

  protected:
    MpiWorld worldA;
    std::string userA = "userA";
    std::string funcA = "funcA";
    int worldIdA = 123;
    int worldSizeA = 3;

    MpiWorld worldB;
    std::string userB = "userB";
    std::string funcB = "funcB";
    int worldIdB = 245;
    int worldSizeB = 3;
};

TEST_CASE_METHOD(MpiBaseTestFixture, "Test creating two MPI worlds", "[mpi]")
{
    // Create the world
    MpiWorld worldA;
    std::string userA = "userA";
    std::string funcA = "funcA";
    int worldIdA = 123;
    int worldSizeA = 3;
    auto msgA = faabric::util::messageFactory(userA, funcA);
    msgA.set_mpiworldid(worldIdA);
    msgA.set_mpiworldsize(worldSizeA);
    worldA.create(msgA, worldIdA, worldSizeA);

    MpiWorld worldB;
    std::string userB = "userB";
    std::string funcB = "funcB";
    int worldIdB = 245;
    int worldSizeB = 6;
    auto msgB = faabric::util::messageFactory(userB, funcB);
    msgB.set_mpiworldid(worldIdB);
    msgB.set_mpiworldsize(worldSizeB);
    worldB.create(msgB, worldIdB, worldSizeB);

    // Check getters on worlds
    REQUIRE(worldA.getSize() == worldSizeA);
    REQUIRE(worldA.getId() == worldIdA);
    REQUIRE(worldA.getUser() == userA);
    REQUIRE(worldA.getFunction() == funcA);
    REQUIRE(worldB.getSize() == worldSizeB);
    REQUIRE(worldB.getId() == worldIdB);
    REQUIRE(worldB.getUser() == userB);
    REQUIRE(worldB.getFunction() == funcB);

    // Check that chained function calls are made as expected
    std::vector<faabric::Message> actual = sch.getRecordedMessagesAll();
    int expectedMsgCount = worldSizeA + worldSizeB - 2;
    REQUIRE(actual.size() == expectedMsgCount);

    for (int i = 0; i < expectedMsgCount; i++) {
        faabric::Message actualCall = actual.at(i);
        if (i < worldSizeA - 1) {
            REQUIRE(actualCall.user() == userA);
            REQUIRE(actualCall.function() == funcA);
            REQUIRE(actualCall.ismpi());
            REQUIRE(actualCall.mpiworldid() == worldIdA);
            REQUIRE(actualCall.mpirank() == i + 1);
            REQUIRE(actualCall.mpiworldsize() == worldSizeA);
        } else {
            REQUIRE(actualCall.user() == userB);
            REQUIRE(actualCall.function() == funcB);
            REQUIRE(actualCall.ismpi());
            REQUIRE(actualCall.mpiworldid() == worldIdB);
            REQUIRE(actualCall.mpirank() == i + 2 - worldSizeA);
            REQUIRE(actualCall.mpiworldsize() == worldSizeB);
        }
    }

    // Check that this host is registered as the master
    const std::string actualHostA = worldA.getHostForRank(0);
    const std::string actualHostB = worldB.getHostForRank(0);
    REQUIRE(actualHostA == faabric::util::getSystemConfig().endpointHost);
    REQUIRE(actualHostB == faabric::util::getSystemConfig().endpointHost);

    worldA.destroy();
    worldB.destroy();
}

TEST_CASE_METHOD(MultiWorldMpiTestFixture,
                 "Test send and recv on same host from multiple worlds",
                 "[mpi]")
{
    // Send a message between colocated ranks
    int rankA1 = 0;
    int rankA2 = 1;
    std::vector<int> messageData = { 0, 1, 2 };
    worldA.send(
      rankA1, rankA2, BYTES(messageData.data()), MPI_INT, messageData.size());
    worldB.send(
      rankA1, rankA2, BYTES(messageData.data()), MPI_INT, messageData.size());

    SECTION("Test queueing")
    {
        // Check for world A
        REQUIRE(worldA.getLocalQueueSize(rankA1, rankA2) == 1);
        REQUIRE(worldA.getLocalQueueSize(rankA2, rankA1) == 0);
        REQUIRE(worldA.getLocalQueueSize(rankA1, 0) == 0);
        REQUIRE(worldA.getLocalQueueSize(rankA2, 0) == 0);
        const std::shared_ptr<InMemoryMpiQueue>& queueA2 =
          worldA.getLocalQueue(rankA1, rankA2);
        faabric::MPIMessage actualMessage = *(queueA2->dequeue());
        // checkMessage(actualMessage, worldId, rankA1, rankA2, messageData);

        // Check for world B
        REQUIRE(worldB.getLocalQueueSize(rankA1, rankA2) == 1);
        REQUIRE(worldB.getLocalQueueSize(rankA2, rankA1) == 0);
        REQUIRE(worldB.getLocalQueueSize(rankA1, 0) == 0);
        REQUIRE(worldB.getLocalQueueSize(rankA2, 0) == 0);
        const std::shared_ptr<InMemoryMpiQueue>& queueA2B =
          worldB.getLocalQueue(rankA1, rankA2);
        actualMessage = *(queueA2B->dequeue());
        // checkMessage(actualMessage, worldId, rankA1, rankA2, messageData);
    }

    SECTION("Test recv")
    {
        MPI_Status status{};
        auto bufferAllocation = std::make_unique<int[]>(messageData.size());
        auto* buffer = bufferAllocation.get();

        // Check for world A
        worldA.recv(
          rankA1, rankA2, BYTES(buffer), MPI_INT, messageData.size(), &status);
        std::vector<int> actual(buffer, buffer + messageData.size());
        REQUIRE(actual == messageData);
        REQUIRE(status.MPI_ERROR == MPI_SUCCESS);
        REQUIRE(status.MPI_SOURCE == rankA1);
        REQUIRE(status.bytesSize == messageData.size() * sizeof(int));

        // Check for world B
        worldB.recv(
          rankA1, rankA2, BYTES(buffer), MPI_INT, messageData.size(), &status);
        std::vector<int> actualB(buffer, buffer + messageData.size());
        REQUIRE(actualB == messageData);
        REQUIRE(status.MPI_ERROR == MPI_SUCCESS);
        REQUIRE(status.MPI_SOURCE == rankA1);
        REQUIRE(status.bytesSize == messageData.size() * sizeof(int));
    }
}
}
