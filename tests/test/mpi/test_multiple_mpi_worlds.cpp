#include <catch2/catch.hpp>

#include "faabric_utils.h"
#include "fixtures.h"

using namespace faabric::mpi;
using namespace faabric::scheduler;

namespace tests {
class MultiWorldMpiTestFixture : public MpiBaseTestFixture
{
  public:
    MultiWorldMpiTestFixture()
    {
        reqA = faabric::util::batchExecFactory(userA, funcA, 1);
        reqB = faabric::util::batchExecFactory(userB, funcB, 1);
        auto& msgA = *reqA->mutable_messages(0);
        auto& msgB = *reqB->mutable_messages(0);
        msgA.set_mpiworldsize(worldSizeA);
        msgA.set_mpiworldid(worldIdA);
        msgB.set_mpiworldsize(worldSizeB);
        msgB.set_mpiworldid(worldIdB);

        // Call the requests once to keep track of the first MPI message
        plannerCli.callFunctions(reqA);
        plannerCli.callFunctions(reqB);

        worldA.create(msgA, worldIdA, worldSizeA);
        worldB.create(msgB, worldIdB, worldSizeB);
    }

    ~MultiWorldMpiTestFixture()
    {
        worldA.destroy();
        worldB.destroy();

        waitForMpiMessages(reqA, worldSizeA);
        waitForMpiMessages(reqB, worldSizeB);
    }

  protected:
    MpiWorld worldA;
    std::shared_ptr<BatchExecuteRequest> reqA = nullptr;
    std::string userA = "userA";
    std::string funcA = "funcA";
    int worldIdA = 123;
    int worldSizeA = 3;

    MpiWorld worldB;
    std::shared_ptr<BatchExecuteRequest> reqB = nullptr;
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
    auto reqA = faabric::util::batchExecFactory(userA, funcA, 1);
    auto& msgA = *reqA->mutable_messages(0);
    msgA.set_ismpi(true);
    msgA.set_mpiworldid(worldIdA);
    msgA.set_mpiworldsize(worldSizeA);
    plannerCli.callFunctions(reqA);
    worldA.create(msgA, worldIdA, worldSizeA);

    MpiWorld worldB;
    std::string userB = "userB";
    std::string funcB = "funcB";
    int worldIdB = 245;
    int worldSizeB = 6;
    auto reqB = faabric::util::batchExecFactory(userB, funcB, 1);
    auto& msgB = *reqB->mutable_messages(0);
    msgB.set_ismpi(true);
    msgB.set_mpiworldid(worldIdB);
    msgB.set_mpiworldsize(worldSizeB);
    plannerCli.callFunctions(reqB);
    worldB.create(msgB, worldIdB, worldSizeB);

    // Wait to make sure all messages are delivered
    waitForMpiMessages(reqA, worldSizeA);
    waitForMpiMessages(reqB, worldSizeB);

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
    auto actual = sch.getRecordedMessages();
    int expectedMsgCount = worldSizeA + worldSizeB;
    REQUIRE(actual.size() == expectedMsgCount);

    // Sort the messages by world and by rank so that we don't have races
    // between messages from different ranks
    std::vector<Message> worldAMsg(worldSizeA);
    std::vector<Message> worldBMsg(worldSizeB);
    for (const auto& msg : actual) {
        if (msg.mpiworldid() == worldIdA) {
            worldAMsg.at(msg.mpirank()) = msg;
        } else {
            worldBMsg.at(msg.mpirank()) = msg;
        }
    }

    for (int i = 0; i < worldSizeA; i++) {
        REQUIRE(worldAMsg.at(i).user() == userA);
        REQUIRE(worldAMsg.at(i).function() == funcA);
        REQUIRE(worldAMsg.at(i).ismpi());
        REQUIRE(worldAMsg.at(i).mpiworldid() == worldIdA);
        REQUIRE(worldAMsg.at(i).mpirank() == i);
        REQUIRE(worldAMsg.at(i).mpiworldsize() == worldSizeA);
    }

    for (int i = 0; i < worldSizeB; i++) {
        REQUIRE(worldBMsg.at(i).user() == userB);
        REQUIRE(worldBMsg.at(i).function() == funcB);
        REQUIRE(worldBMsg.at(i).ismpi());
        REQUIRE(worldBMsg.at(i).mpiworldid() == worldIdB);
        REQUIRE(worldBMsg.at(i).mpirank() == i);
        REQUIRE(worldBMsg.at(i).mpiworldsize() == worldSizeB);
    }

    // Check that this host is registered as the main
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
        MpiMessage actualMessage = queueA2->dequeue();
        // checkMessage(actualMessage, worldId, rankA1, rankA2, messageData);

        // Check for world B
        REQUIRE(worldB.getLocalQueueSize(rankA1, rankA2) == 1);
        REQUIRE(worldB.getLocalQueueSize(rankA2, rankA1) == 0);
        REQUIRE(worldB.getLocalQueueSize(rankA1, 0) == 0);
        REQUIRE(worldB.getLocalQueueSize(rankA2, 0) == 0);
        const std::shared_ptr<InMemoryMpiQueue>& queueA2B =
          worldB.getLocalQueue(rankA1, rankA2);
        actualMessage = queueA2B->dequeue();
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
