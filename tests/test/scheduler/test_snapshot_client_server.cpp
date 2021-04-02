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

TEST_CASE("Test pushing snapshot", "[scheduler]")
{
    cleanFaabric();

    // Start the server
    ServerContext serverContext;
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
    snapA.data = new uint8_t[snapSizeA];
    snapB.data = new uint8_t[snapSizeB];

    // Send the message
    scheduler::SnapshotClient cli(LOCALHOST);
    cli.pushSnapshot(snapKeyA, snapA);
    cli.pushSnapshot(snapKeyB, snapB);

    // Check snapshots created in regsitry
    REQUIRE(registry.getSnapshotCount() == 2);

    // Stop the server
    server.stop();
}
}
