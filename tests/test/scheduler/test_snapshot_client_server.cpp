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

class SnapshotClientServerFixture : public BaseTestFixture
{
  protected:
    faabric::scheduler::SnapshotServer server;
    faabric::scheduler::SnapshotClient cli;
    snapshot::SnapshotRegistry& registry;

  public:
    SnapshotClientServerFixture()
      : cli(LOCALHOST)
      , registry(snapshot::getSnapshotRegistry())
    {
        registry.clear();

        server.start();
        usleep(1000 * SHORT_TEST_TIMEOUT_MS);
    }

    ~SnapshotClientServerFixture()
    {
        cli.close();
        server.stop();
        registry.clear();
    }
};

TEST_CASE_METHOD(SnapshotClientServerFixture,
                 "Test pushing and deleting snapshots",
                 "[scheduler]")
{
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

    std::vector<uint8_t> dataA(snapSizeA, 1);
    std::vector<uint8_t> dataB(snapSizeB, 2);

    snapA.data = dataA.data();
    snapB.data = dataB.data();

    // Send the message
    cli.pushSnapshot(snapKeyA, snapA);
    cli.pushSnapshot(snapKeyB, snapB);

    usleep(1000 * 500);

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
}

TEST_CASE_METHOD(SnapshotClientServerFixture,
                 "Test set thread result",
                 "[scheduler]")
{
    // Register threads on this host
    int threadIdA = 123;
    int threadIdB = 345;
    int returnValueA = 88;
    int returnValueB = 99;
    sch.registerThread(threadIdA);
    sch.registerThread(threadIdB);

    // Set up two threads to await the results
    std::thread tA([threadIdA, returnValueA] {
        faabric::scheduler::Scheduler& sch = faabric::scheduler::getScheduler();
        int32_t r = sch.awaitThreadResult(threadIdA);
        REQUIRE(r == returnValueA);
    });

    std::thread tB([threadIdB, returnValueB] {
        faabric::scheduler::Scheduler& sch = faabric::scheduler::getScheduler();
        int32_t r = sch.awaitThreadResult(threadIdB);
        REQUIRE(r == returnValueB);
    });

    cli.pushThreadResult(threadIdA, returnValueA);
    cli.pushThreadResult(threadIdB, returnValueB);

    if (tA.joinable()) {
        tA.join();
    }

    if (tB.joinable()) {
        tB.join();
    }
}
}
