#include <catch2/catch.hpp>

#include "faabric_utils.h"

#include <faabric/scheduler/MpiWorld.h>
#include <faabric/util/exec_graph.h>
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
}
