#include <catch2/catch.hpp>

#include "faabric_utils.h"

#include <faabric/scheduler/MpiWorld.h>
#include <faabric/util/exec_graph.h>
#include <faabric/util/json.h>
#include <faabric/util/macros.h>

namespace tests {
TEST_CASE_METHOD(MpiTestFixture,
                 "Test tracing the number of MPI messages",
                 "[util][exec-graph]")
{
    msg.set_recordexecgraph(true);

    // Send one message
    int rankA1 = 0;
    int rankA2 = 1;
    MPI_Status status{};

    std::vector<int> messageData = { 0, 1, 2 };
    auto buffer = new int[messageData.size()];

    int numToSend = 10;
    std::string expectedKey =
      faabric::util::exec_graph::mpiMsgCountPrefix + std::to_string(rankA2);

    for (int i = 0; i < numToSend; i++) {
        world.send(rankA1,
                   rankA2,
                   BYTES(messageData.data()),
                   MPI_INT,
                   messageData.size());
        world.recv(
          rankA1, rankA2, BYTES(buffer), MPI_INT, messageData.size(), &status);
    }

    REQUIRE(msg.intexecgraphdetails_size() == 1);
    REQUIRE(msg.execgraphdetails_size() == 0);
    REQUIRE(msg.intexecgraphdetails().count(expectedKey) == 1);
    REQUIRE(msg.intexecgraphdetails().at(expectedKey) == numToSend);
}

TEST_CASE_METHOD(MpiTestFixture,
                 "Test tracing is disabled if flag in message not set",
                 "[util][exec-graph]")
{
    // Disable test mode and set message flag to true
    msg.set_recordexecgraph(false);

    // Send one message
    int rankA1 = 0;
    int rankA2 = 1;
    MPI_Status status{};

    std::vector<int> messageData = { 0, 1, 2 };
    auto buffer = new int[messageData.size()];

    int numToSend = 10;
    std::string expectedKey =
      faabric::util::exec_graph::mpiMsgCountPrefix + std::to_string(rankA2);

    for (int i = 0; i < numToSend; i++) {
        world.send(rankA1,
                   rankA2,
                   BYTES(messageData.data()),
                   MPI_INT,
                   messageData.size());
        world.recv(
          rankA1, rankA2, BYTES(buffer), MPI_INT, messageData.size(), &status);
    }

    // Stop recording and check we have recorded no message
    REQUIRE(msg.intexecgraphdetails_size() == 0);
    REQUIRE(msg.execgraphdetails_size() == 0);
}

TEST_CASE_METHOD(MpiBaseTestFixture,
                 "Test different threads populate the graph",
                 "[util][exec-graph]")
{
    int rank = 0;
    int otherRank = 1;
    int worldSize = 2;
    int worldId = 123;

    faabric::Message msg = faabric::util::messageFactory("mpi", "hellompi");
    msg.set_ismpi(true);
    msg.set_recordexecgraph(true);
    msg.set_mpiworldsize(worldSize);
    msg.set_mpiworldid(worldId);

    faabric::Message otherMsg = msg;
    otherMsg.set_mpirank(otherRank);
    msg.set_mpirank(rank);

    faabric::scheduler::MpiWorld& thisWorld =
      faabric::scheduler::getMpiWorldRegistry().createWorld(msg, worldId);

    std::vector<int> messageData = { 0, 1, 2 };
    auto buffer = new int[messageData.size()];
    std::thread otherWorldThread([&messageData, &otherMsg, rank, otherRank] {
        faabric::scheduler::MpiWorld& otherWorld =
          faabric::scheduler::getMpiWorldRegistry().getOrInitialiseWorld(
            otherMsg);

        otherWorld.send(otherRank,
                        rank,
                        BYTES(messageData.data()),
                        MPI_INT,
                        messageData.size());

        otherWorld.destroy();
    });

    thisWorld.recv(
      otherRank, rank, BYTES(buffer), MPI_INT, messageData.size(), nullptr);

    thisWorld.destroy();

    if (otherWorldThread.joinable()) {
        otherWorldThread.join();
    }

    std::string expectedKey =
      faabric::util::exec_graph::mpiMsgCountPrefix + std::to_string(rank);
    REQUIRE(otherMsg.mpirank() == otherRank);
    REQUIRE(otherMsg.intexecgraphdetails_size() == 1);
    REQUIRE(otherMsg.intexecgraphdetails().count(expectedKey) == 1);
    REQUIRE(otherMsg.intexecgraphdetails().at(expectedKey) == 1);
}
}
