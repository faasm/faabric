#include "faabric_utils.h"
#include <catch.hpp>

#include <faabric/scheduler/SnapshotClient.h>
#include <faabric/scheduler/SnapshotServer.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/util/config.h>
#include <faabric/util/environment.h>
#include <faabric/util/network.h>
#include <faabric/util/testing.h>

namespace tests {

TEST_CASE("Test pushing and deleting snapshots", "[scheduler]")
{
    cleanFaabric();

    // Start the server
    scheduler::SnapshotServer server;
    server.start();
    usleep(1000 * 100);

    snapshot::SnapshotRegistry& registry = snapshot::getSnapshotRegistry();

    // Check nothing to start with
    REQUIRE(registry.getSnapshotCount() == 0);

    // Prepare some snapshot data
    std::string snapKeyA = "foo";
    std::string snapKeyB = "bar";
    faabric::util::SnapshotData snapA;
    faabric::util::SnapshotData snapB;
    size_t snapSizeA = 1024;
    size_t snapSizeB = 500;
    snapA.size = snapSizeA;
    snapB.size = snapSizeB;

    std::vector<uint8_t> dataA = { 0, 1, 2, 3, 4 };
    std::vector<uint8_t> dataB = { 3, 3, 2, 2 };

    snapA.data = dataA.data();
    snapB.data = dataB.data();

    // Send the message
    scheduler::SnapshotClient cli(LOCALHOST);
    cli.pushSnapshot(snapKeyA, snapA);
    cli.pushSnapshot(snapKeyB, snapB);
    usleep(1000 * 100);

    // Check snapshots created in regsitry
    REQUIRE(registry.getSnapshotCount() == 2);
    const faabric::util::SnapshotData& actualA = registry.getSnapshot(snapKeyA);
    const faabric::util::SnapshotData& actualB = registry.getSnapshot(snapKeyB);

    REQUIRE(actualA.size == snapA.size);
    REQUIRE(actualB.size == snapB.size);

    std::vector<uint8_t> actualDataA(actualA.data, actualA.data + dataA.size());
    std::vector<uint8_t> actualDataB(actualB.data, actualB.data + dataB.size());

    REQUIRE(actualDataA == dataA);
    REQUIRE(actualDataB == dataB);

    // Close the client
    cli.close();
    // Stop the server
    server.stop();
}
}
