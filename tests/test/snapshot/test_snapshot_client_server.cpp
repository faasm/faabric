#include <catch2/catch.hpp>

#include "faabric_utils.h"
#include "fixtures.h"

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

using namespace faabric::util;

namespace tests {

class SnapshotClientServerFixture
  : public SchedulerTestFixture
  , public RedisTestFixture
  , public SnapshotTestFixture
  , public PointToPointTestFixture
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

    void setUpFunctionGroup(int appId, int groupId)
    {
        SchedulingDecision decision(appId, groupId);
        faabric::Message msg = messageFactory("foo", "bar");
        msg.set_appid(appId);
        msg.set_groupid(groupId);

        decision.addMessage(LOCALHOST, msg);
        broker.setUpLocalMappingsFromSchedulingDecision(decision);
    }
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
    SnapshotData snapA;
    SnapshotData snapB;
    size_t snapSizeA = 1024;
    size_t snapSizeB = 500;
    snapA.size = snapSizeA;
    snapB.size = snapSizeB;

    std::vector<uint8_t> dataA(snapSizeA, 1);
    std::vector<uint8_t> dataB(snapSizeB, 2);

    snapA.data = dataA.data();
    snapB.data = dataB.data();

    // One request with no group
    int appId = 111;
    int groupIdA = 0;
    int groupIdB = 123;

    setUpFunctionGroup(appId, groupIdB);

    // Send the message
    cli.pushSnapshot(snapKeyA, groupIdA, snapA);
    cli.pushSnapshot(snapKeyB, groupIdB, snapB);

    // Check snapshots created in registry
    REQUIRE(reg.getSnapshotCount() == 2);
    const SnapshotData& actualA = reg.getSnapshot(snapKeyA);
    const SnapshotData& actualB = reg.getSnapshot(snapKeyB);

    REQUIRE(actualA.size == snapA.size);
    REQUIRE(actualB.size == snapB.size);

    std::vector<uint8_t> actualDataA(actualA.data, actualA.data + dataA.size());
    std::vector<uint8_t> actualDataB(actualB.data, actualB.data + dataB.size());

    REQUIRE(actualDataA == dataA);
    REQUIRE(actualDataB == dataB);
}

void checkDiffsApplied(const uint8_t* snapBase, std::vector<SnapshotDiff> diffs)
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
    std::string thisHost = getSystemConfig().endpointHost;

    // One request with no group, another with a group we must initialise
    int appId = 111;
    int groupIdA = 0;
    int groupIdB = 234;

    setUpFunctionGroup(appId, groupIdB);

    // Set up a snapshot
    std::string snapKey = std::to_string(generateGid());
    SnapshotData snap = takeSnapshot(snapKey, 5, true);

    // Set up some diffs
    std::vector<uint8_t> diffDataA1 = { 0, 1, 2, 3 };
    std::vector<uint8_t> diffDataA2 = { 4, 5, 6 };
    std::vector<uint8_t> diffDataB = { 7, 7, 8, 8, 8 };

    std::vector<SnapshotDiff> diffsA;
    std::vector<SnapshotDiff> diffsB;

    size_t originalDiffsApplied = server.diffsApplied();

    SnapshotDiff diffA1(SnapshotDataType::Raw,
                        SnapshotMergeOperation::Overwrite,
                        5,
                        diffDataA1.data(),
                        diffDataA1.size());

    SnapshotDiff diffA2(SnapshotDataType::Raw,
                        SnapshotMergeOperation::Overwrite,
                        2 * HOST_PAGE_SIZE,
                        diffDataA2.data(),
                        diffDataA2.size());

    diffsA = { diffA1, diffA2 };
    cli.pushSnapshotDiffs(snapKey, groupIdA, diffsA);

    SnapshotDiff diffB(SnapshotDataType::Raw,
                       SnapshotMergeOperation::Overwrite,
                       3 * HOST_PAGE_SIZE,
                       diffDataB.data(),
                       diffDataB.size());
    diffsB = { diffB };
    cli.pushSnapshotDiffs(snapKey, groupIdB, diffsB);

    // Ensure the right number of diffs is applied
    // Also acts as a memory barrier for TSan
    REQUIRE(server.diffsApplied() == originalDiffsApplied + 3);

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
    std::string snapKey = std::to_string(generateGid());
    SnapshotData snap = takeSnapshot(snapKey, 5, false);

    // Set up a couple of ints in the snapshot
    int offsetA1 = 8;
    int offsetA2 = 2 * HOST_PAGE_SIZE;
    int baseA1 = 25;
    int baseA2 = 60;

    int* basePtrA1 = (int*)(snap.data + offsetA1);
    int* basePtrA2 = (int*)(snap.data + offsetA2);
    *basePtrA1 = baseA1;
    *basePtrA2 = baseA2;

    // Set up some diffs with different merge operations
    int diffIntA1 = 123;
    int diffIntA2 = 345;

    std::vector<uint8_t> intDataA1 = valueToBytes<int>(diffIntA1);
    std::vector<uint8_t> intDataA2 = valueToBytes<int>(diffIntA2);

    std::vector<SnapshotDiff> diffs;

    SnapshotDiff diffA1(SnapshotDataType::Raw,
                        SnapshotMergeOperation::Overwrite,
                        offsetA1,
                        intDataA1.data(),
                        intDataA1.size());
    diffA1.operation = SnapshotMergeOperation::Sum;
    diffA1.dataType = SnapshotDataType::Int;

    SnapshotDiff diffA2(SnapshotDataType::Raw,
                        SnapshotMergeOperation::Overwrite,
                        offsetA2,
                        intDataA2.data(),
                        intDataA2.size());
    diffA2.operation = SnapshotMergeOperation::Sum;
    diffA2.dataType = SnapshotDataType::Int;

    size_t originalDiffsApplied = server.diffsApplied();

    diffs = { diffA1, diffA2 };
    cli.pushSnapshotDiffs(snapKey, 0, diffs);

    // Ensure the right number of diffs is applied
    // Also acts as a memory barrier for TSan
    REQUIRE(server.diffsApplied() == originalDiffsApplied + 2);

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
    std::string snapKey = std::to_string(generateGid());
    SnapshotData snap = takeSnapshot(snapKey, 5, false);

    int offset = 8;
    std::vector<uint8_t> originalData;
    std::vector<uint8_t> diffData;
    std::vector<uint8_t> expectedData;

    SnapshotMergeOperation operation = SnapshotMergeOperation::Overwrite;
    SnapshotDataType dataType = SnapshotDataType::Raw;

    SECTION("Integer")
    {
        dataType = SnapshotDataType::Int;
        int original = 0;
        int diff = 0;
        int expected = 0;

        SECTION("Sum")
        {
            original = 100;
            diff = 10;
            expected = 110;

            operation = SnapshotMergeOperation::Sum;
        }

        SECTION("Subtract")
        {
            original = 100;
            diff = 10;
            expected = 90;

            operation = SnapshotMergeOperation::Subtract;
        }

        SECTION("Product")
        {
            original = 10;
            diff = 20;
            expected = 200;

            operation = SnapshotMergeOperation::Product;
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

            operation = SnapshotMergeOperation::Min;
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

            operation = SnapshotMergeOperation::Max;
        }

        originalData = valueToBytes<int>(original);
        diffData = valueToBytes<int>(diff);
        expectedData = valueToBytes<int>(expected);
    }

    // Put original data in place
    std::memcpy(snap.data + offset, originalData.data(), originalData.size());

    SnapshotDiff diff(SnapshotDataType::Raw,
                      SnapshotMergeOperation::Overwrite,
                      offset,
                      diffData.data(),
                      diffData.size());
    diff.operation = operation;
    diff.dataType = dataType;

    size_t originalDiffsApplied = server.diffsApplied();

    std::vector<SnapshotDiff> diffs = { diff };
    cli.pushSnapshotDiffs(snapKey, 0, diffs);

    // Ensure the right number of diffs is applied
    // Also acts as a memory barrier for TSan
    REQUIRE(server.diffsApplied() == originalDiffsApplied + 1);

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
        assert(r == returnValueA);
    });

    std::thread tB([threadIdB, returnValueB] {
        faabric::scheduler::Scheduler& sch = faabric::scheduler::getScheduler();
        int32_t r = sch.awaitThreadResult(threadIdB);
        assert(r == returnValueB);
    });

    if (tA.joinable()) {
        tA.join();
    }

    if (tB.joinable()) {
        tB.join();
    }
}
}
