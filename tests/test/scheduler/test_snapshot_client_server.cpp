#include "faabric_utils.h"
#include "fixtures.h"
#include <catch.hpp>

#include <sys/mman.h>

#include <faabric/scheduler/SnapshotClient.h>
#include <faabric/scheduler/SnapshotServer.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/util/config.h>
#include <faabric/util/environment.h>
#include <faabric/util/gids.h>
#include <faabric/util/network.h>
#include <faabric/util/testing.h>

namespace tests {

class SnapshotClientServerFixture
  : public SchedulerTestFixture
  , public RedisTestFixture
  , public SnapshotTestFixture
{
  protected:
    faabric::scheduler::SnapshotServer server;
    faabric::scheduler::SnapshotClient cli;

  public:
    SnapshotClientServerFixture()
      : cli(LOCALHOST)
    {
        server.start();
        usleep(1000 * SHORT_TEST_TIMEOUT_MS);
    }

    ~SnapshotClientServerFixture() { server.stop(); }
};

TEST_CASE_METHOD(SnapshotClientServerFixture,
                 "Test pushing and deleting snapshots",
                 "[scheduler]")
{
    // Check nothing to start with
    REQUIRE(reg.getSnapshotCount() == 0);

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

    // Check snapshots created in registry
    REQUIRE(reg.getSnapshotCount() == 2);
    const faabric::util::SnapshotData& actualA = reg.getSnapshot(snapKeyA);
    const faabric::util::SnapshotData& actualB = reg.getSnapshot(snapKeyB);

    REQUIRE(actualA.size == snapA.size);
    REQUIRE(actualB.size == snapB.size);

    std::vector<uint8_t> actualDataA(actualA.data, actualA.data + dataA.size());
    std::vector<uint8_t> actualDataB(actualB.data, actualB.data + dataB.size());

    REQUIRE(actualDataA == dataA);
    REQUIRE(actualDataB == dataB);
}

void checkDiffsApplied(const uint8_t* snapBase,
                       std::vector<faabric::util::SnapshotDiff> diffs)
{
    for (const auto& d : diffs) {
        std::vector<uint8_t> actual(snapBase + d.offset,
                                    snapBase + d.offset + d.size);

        std::vector<uint8_t> expected(d.data, d.data + d.size);

        REQUIRE(actual == expected);
    }
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

    // Set up a snapshot
    faabric::util::SnapshotData snap;
    snap.size = 5 * faabric::util::HOST_PAGE_SIZE;
    snap.data = (uint8_t*)mmap(
      nullptr, snap.size, PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);

    std::string snapKey = std::to_string(faabric::util::generateGid());
    reg.takeSnapshot(snapKey, snap);

    // Set up some diffs
    std::vector<uint8_t> diffDataA1 = { 0, 1, 2, 3 };
    std::vector<uint8_t> diffDataA2 = { 4, 5, 6 };
    std::vector<uint8_t> diffDataB = { 7, 7, 8, 8, 8 };

    std::vector<faabric::util::SnapshotDiff> diffsA;
    std::vector<faabric::util::SnapshotDiff> diffsB;

    SECTION("Without diffs")
    {
        cli.pushThreadResult(threadIdA, returnValueA);
        cli.pushThreadResult(threadIdB, returnValueB);
    }

    SECTION("Empty diffs")
    {
        cli.pushThreadResult(threadIdA, returnValueA, snapKey, diffsA);
        cli.pushThreadResult(threadIdB, returnValueB, snapKey, diffsB);
    }

    SECTION("With diffs")
    {
        faabric::util::SnapshotDiff diffA1(
          5, diffDataA1.data(), diffDataA1.size());
        faabric::util::SnapshotDiff diffA2(2 * faabric::util::HOST_PAGE_SIZE,
                                           diffDataA2.data(),
                                           diffDataA2.size());
        diffsA = { diffA1, diffA2 };
        cli.pushThreadResult(threadIdA, returnValueA, snapKey, diffsA);

        faabric::util::SnapshotDiff diffB(3 * faabric::util::HOST_PAGE_SIZE,
                                          diffDataB.data(),
                                          diffDataB.size());
        diffsB = { diffB };
        cli.pushThreadResult(threadIdB, returnValueB, snapKey, diffsB);
    }

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

    if (tA.joinable()) {
        tA.join();
    }

    if (tB.joinable()) {
        tB.join();
    }

    // Check changes have been applied
    checkDiffsApplied(snap.data, diffsA);
    checkDiffsApplied(snap.data, diffsB);

    munmap(snap.data, snap.size);
}
}
