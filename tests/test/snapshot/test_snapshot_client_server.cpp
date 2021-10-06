#include "faabric_utils.h"
#include "fixtures.h"
#include <catch.hpp>

#include <sys/mman.h>

#include <faabric/snapshot/SnapshotClient.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/snapshot/SnapshotServer.h>
#include <faabric/util/bytes.h>
#include <faabric/util/config.h>
#include <faabric/util/environment.h>
#include <faabric/util/gids.h>
#include <faabric/util/macros.h>
#include <faabric/util/network.h>
#include <faabric/util/snapshot.h>
#include <faabric/util/testing.h>

namespace tests {

class SnapshotClientServerFixture
  : public SchedulerTestFixture
  , public RedisTestFixture
  , public SnapshotTestFixture
{
  protected:
    faabric::snapshot::SnapshotServer server;
    faabric::snapshot::SnapshotClient cli;

  public:
    SnapshotClientServerFixture()
      : cli(LOCALHOST)
    {
        server.start();
    }

    ~SnapshotClientServerFixture() { server.stop(); }
};

TEST_CASE_METHOD(ConfTestFixture,
                 "Test setting snapshot server threads",
                 "[snapshot]")
{
    conf.snapshotServerThreads = 5;

    faabric::snapshot::SnapshotServer server;

    REQUIRE(server.getNThreads() == 5);
}

TEST_CASE_METHOD(SnapshotClientServerFixture,
                 "Test pushing and deleting snapshots",
                 "[snapshot]")
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
                 "Test push snapshot diffs",
                 "[snapshot]")
{
    // Set up a snapshot
    std::string snapKey = std::to_string(faabric::util::generateGid());
    faabric::util::SnapshotData snap = takeSnapshot(snapKey, 5, true);

    // Set up some diffs
    std::vector<uint8_t> diffDataA1 = { 0, 1, 2, 3 };
    std::vector<uint8_t> diffDataA2 = { 4, 5, 6 };
    std::vector<uint8_t> diffDataB = { 7, 7, 8, 8, 8 };

    std::vector<faabric::util::SnapshotDiff> diffsA;
    std::vector<faabric::util::SnapshotDiff> diffsB;

    faabric::util::SnapshotDiff diffA1(5, diffDataA1.data(), diffDataA1.size());
    faabric::util::SnapshotDiff diffA2(
      2 * faabric::util::HOST_PAGE_SIZE, diffDataA2.data(), diffDataA2.size());
    diffsA = { diffA1, diffA2 };
    cli.pushSnapshotDiffs(snapKey, diffsA);

    faabric::util::SnapshotDiff diffB(
      3 * faabric::util::HOST_PAGE_SIZE, diffDataB.data(), diffDataB.size());
    diffsB = { diffB };
    cli.pushSnapshotDiffs(snapKey, diffsB);

    // Check changes have been applied
    checkDiffsApplied(snap.data, diffsA);
    checkDiffsApplied(snap.data, diffsB);

    deallocatePages(snap.data, 5);
}

TEST_CASE_METHOD(SnapshotClientServerFixture,
                 "Test detailed snapshot diffs with merge ops",
                 "[snapshot]")
{
    // Set up a snapshot
    std::string snapKey = std::to_string(faabric::util::generateGid());
    faabric::util::SnapshotData snap = takeSnapshot(snapKey, 5, false);

    // Set up a couple of ints in the snapshot
    int offsetA1 = 5;
    int offsetA2 = 2 * faabric::util::HOST_PAGE_SIZE;
    int baseA1 = 25;
    int baseA2 = 60;

    int* basePtrA1 = (int*)(snap.data + offsetA1);
    int* basePtrA2 = (int*)(snap.data + offsetA2);
    *basePtrA1 = baseA1;
    *basePtrA2 = baseA2;

    // Set up some diffs with different merge operations
    int diffIntA1 = 123;
    int diffIntA2 = 345;

    std::vector<uint8_t> intDataA1 =
      faabric::util::valueToBytes<int>(diffIntA1);
    std::vector<uint8_t> intDataA2 =
      faabric::util::valueToBytes<int>(diffIntA2);

    std::vector<faabric::util::SnapshotDiff> diffs;

    faabric::util::SnapshotDiff diffA1(
      offsetA1, intDataA1.data(), intDataA1.size());
    diffA1.operation = faabric::util::SnapshotMergeOperation::Sum;
    diffA1.dataType = faabric::util::SnapshotDataType::Int;

    faabric::util::SnapshotDiff diffA2(
      offsetA2, intDataA2.data(), intDataA2.size());
    diffA2.operation = faabric::util::SnapshotMergeOperation::Sum;
    diffA2.dataType = faabric::util::SnapshotDataType::Int;

    diffs = { diffA1, diffA2 };
    cli.pushSnapshotDiffs(snapKey, diffs);

    // Check diffs have been applied according to the merge operations
    REQUIRE(*basePtrA1 == baseA1 + diffIntA1);
    REQUIRE(*basePtrA2 == baseA2 + diffIntA2);

    deallocatePages(snap.data, 5);
}

TEST_CASE_METHOD(SnapshotClientServerFixture,
                 "Test snapshot diffs with merge ops",
                 "[snapshot]")
{
    // Set up a snapshot
    std::string snapKey = std::to_string(faabric::util::generateGid());
    faabric::util::SnapshotData snap = takeSnapshot(snapKey, 5, false);

    int offset = 5;
    std::vector<uint8_t> originalData;
    std::vector<uint8_t> diffData;
    std::vector<uint8_t> expectedData;

    faabric::util::SnapshotMergeOperation operation =
      faabric::util::SnapshotMergeOperation::Overwrite;
    faabric::util::SnapshotDataType dataType =
      faabric::util::SnapshotDataType::Raw;

    SECTION("Integer")
    {
        dataType = faabric::util::SnapshotDataType::Int;
        int original = 0;
        int diff = 0;
        int expected = 0;

        SECTION("Sum")
        {
            original = 100;
            diff = 10;
            expected = 110;

            operation = faabric::util::SnapshotMergeOperation::Sum;
        }

        SECTION("Subtract")
        {
            original = 100;
            diff = 10;
            expected = 90;

            operation = faabric::util::SnapshotMergeOperation::Subtract;
        }

        SECTION("Product")
        {
            original = 10;
            diff = 20;
            expected = 200;

            operation = faabric::util::SnapshotMergeOperation::Product;
        }

        SECTION("Min")
        {
            SECTION("With change")
            {
                original = 1000;
                diff = 100;
                expected = 100;
            }

            SECTION("No change")
            {
                original = 10;
                diff = 20;
                expected = 10;
            }

            operation = faabric::util::SnapshotMergeOperation::Min;
        }

        SECTION("Max")
        {
            SECTION("With change")
            {
                original = 100;
                diff = 1000;
                expected = 1000;
            }

            SECTION("No change")
            {
                original = 20;
                diff = 10;
                expected = 20;
            }

            operation = faabric::util::SnapshotMergeOperation::Max;
        }

        originalData = faabric::util::valueToBytes<int>(original);
        diffData = faabric::util::valueToBytes<int>(diff);
        expectedData = faabric::util::valueToBytes<int>(expected);
    }

    // Put original data in place
    std::memcpy(snap.data + offset, originalData.data(), originalData.size());

    faabric::util::SnapshotDiff diff(offset, diffData.data(), diffData.size());
    diff.operation = operation;
    diff.dataType = dataType;

    std::vector<faabric::util::SnapshotDiff> diffs = { diff };
    cli.pushSnapshotDiffs(snapKey, diffs);

    // Check data is as expected
    std::vector<uint8_t> actualData(snap.data + offset,
                                    snap.data + offset + expectedData.size());
    REQUIRE(actualData == expectedData);

    deallocatePages(snap.data, 5);
}

TEST_CASE_METHOD(SnapshotClientServerFixture,
                 "Test set thread result",
                 "[snapshot]")
{
    // Register threads on this host
    int threadIdA = 123;
    int threadIdB = 345;
    int returnValueA = 88;
    int returnValueB = 99;
    sch.registerThread(threadIdA);
    sch.registerThread(threadIdB);

    cli.pushThreadResult(threadIdA, returnValueA);
    cli.pushThreadResult(threadIdB, returnValueB);

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
}
}
