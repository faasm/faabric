#include <catch2/catch.hpp>

#include "faabric_utils.h"

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/MpiWorld.h>
#include <faabric/util/macros.h>
#include <faabric/util/tracing.h>

namespace tests {
TEST_CASE_METHOD(MpiTestFixture,
                 "Test tracing the number of MPI messages",
                 "[util][tracing]")
{
    // Disable test mode to test tracing
    faabric::util::setTestMode(false);

    faabric::util::tracing::getCallRecords().startRecording(msg);

    // Send one message
    int rankA1 = 0;
    int rankA2 = 1;
    MPI_Status status{};

    std::vector<int> messageData = { 0, 1, 2 };
    auto buffer = new int[messageData.size()];

    int numToSend = 10;

    for (int i = 0; i < numToSend; i++) {
        world.send(rankA1,
                   rankA2,
                   BYTES(messageData.data()),
                   MPI_INT,
                   messageData.size());
        world.recv(
          rankA1, rankA2, BYTES(buffer), MPI_INT, messageData.size(), &status);
    }

    // Stop recording and check we have only recorded one message
    faabric::util::tracing::getCallRecords().stopRecording(msg);
    REQUIRE(msg.has_records());
    REQUIRE(msg.records().has_mpimsgcount());
    REQUIRE(msg.records().mpimsgcount().ranks_size() == worldSize);
    for (int i = 0; i < worldSize; i++) {
        if (i == rankA2) {
            REQUIRE(msg.records().mpimsgcount().nummessages(i) == numToSend);
        } else {
            REQUIRE(msg.records().mpimsgcount().nummessages(i) == 0);
        }
    }

    faabric::util::setTestMode(true);
}
}
