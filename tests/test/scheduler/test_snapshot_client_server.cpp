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

    // TODO - check no snapshots

    // Prepare some snapshot data
    std::string snapKey = "foo";
    faabric::util::SnapshotData snap;
    size_t snapSize = 1024;
    snap.size = snapSize;
    snap.data = new uint8_t[snapSize];

    // Send the message
    scheduler::SnapshotClient cli(LOCALHOST);
    cli.pushSnapshot(snapKey, snap);

    // TODO - check snapshot created

    // Stop the server
    server.stop();
}
}
