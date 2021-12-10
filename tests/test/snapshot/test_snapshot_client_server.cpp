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
#include <faabric/util/memory.h>
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
    size_t snapSizeA = 1024;
    size_t snapSizeB = 500;

    std::vector<uint8_t> dataA(snapSizeA, 1);
    std::vector<uint8_t> dataB(snapSizeB, 2);

    auto snapA = std::make_shared<SnapshotData>(dataA);
    auto snapB = std::make_shared<SnapshotData>(dataB);

    int appId = 111;
    int groupIdA = 0;
    int groupIdB = 123;
    setUpFunctionGroup(appId, groupIdB);

    REQUIRE(reg.getSnapshotCount() == 0);

    // Send the messages
    cli.pushSnapshot(snapKeyA, groupIdA, snapA);
    cli.pushSnapshot(snapKeyB, groupIdB, snapB);

    // Check snapshots created in registry
    REQUIRE(reg.getSnapshotCount() == 2);
    const auto actualA = reg.getSnapshot(snapKeyA);
    const auto actualB = reg.getSnapshot(snapKeyB);

    REQUIRE(actualA->size == snapA->size);
    REQUIRE(actualB->size == snapB->size);

    std::vector<uint8_t> actualDataA = actualA->getDataCopy();
    std::vector<uint8_t> actualDataB = actualB->getDataCopy();

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

    // Set up a snapshot that's got enough memory to expand into
    std::string snapKey = std::to_string(generateGid());
    size_t initialSnapSize = 5 * HOST_PAGE_SIZE;
    size_t expandedSnapSize = 10 * HOST_PAGE_SIZE;

    auto snap =
      std::make_shared<SnapshotData>(initialSnapSize, expandedSnapSize);
    snap->makeRestorable(snapKey);

    // Set up the snapshot
    reg.registerSnapshot(snapKey, snap);

    // Set up some diffs for the initial request
    uint32_t offsetA1 = 5;
    uint32_t offsetA2 = 2 * HOST_PAGE_SIZE;
    std::vector<uint8_t> diffDataA1 = { 0, 1, 2, 3 };
    std::vector<uint8_t> diffDataA2 = { 4, 5, 6 };

    REQUIRE(server.diffsApplied() == 0);

    SnapshotDiff diffA1(SnapshotDataType::Raw,
                        SnapshotMergeOperation::Overwrite,
                        offsetA1,
                        diffDataA1.data(),
                        diffDataA1.size());

    SnapshotDiff diffA2(SnapshotDataType::Raw,
                        SnapshotMergeOperation::Overwrite,
                        offsetA2,
                        diffDataA2.data(),
                        diffDataA2.size());

    std::vector<SnapshotDiff> diffsA = { diffA1, diffA2 };
    cli.pushSnapshotDiffs(snapKey, groupIdA, diffsA);
    REQUIRE(server.diffsApplied() == 2);

    // Submit some more diffs, some larger than the original snapshot (to check
    // it gets extended)
    uint32_t offsetB1 = 3 * HOST_PAGE_SIZE;
    uint32_t offsetB2 = initialSnapSize + 10;
    uint32_t offsetB3 = initialSnapSize + (3 * HOST_PAGE_SIZE);

    std::vector<uint8_t> diffDataB1 = { 7, 7, 8, 8, 8 };
    std::vector<uint8_t> diffDataB2 = { 5, 5, 5, 5 };
    std::vector<uint8_t> diffDataB3 = { 1, 1, 2, 2, 3, 3, 4, 4 };

    SnapshotDiff diffB1(SnapshotDataType::Raw,
                        SnapshotMergeOperation::Overwrite,
                        offsetB1,
                        diffDataB1.data(),
                        diffDataB1.size());

    SnapshotDiff diffB2(SnapshotDataType::Raw,
                        SnapshotMergeOperation::Overwrite,
                        offsetB2,
                        diffDataB2.data(),
                        diffDataB2.size());

    SnapshotDiff diffB3(SnapshotDataType::Raw,
                        SnapshotMergeOperation::Overwrite,
                        offsetB3,
                        diffDataB3.data(),
                        diffDataB3.size());

    std::vector<SnapshotDiff> diffsB = { diffB1, diffB2, diffB3 };
    cli.pushSnapshotDiffs(snapKey, groupIdB, diffsB);

    // Ensure the right number of diffs is applied
    // Also acts as a memory barrier for TSan
    REQUIRE(server.diffsApplied() == 5);

    // Check changes have been applied
    checkDiffsApplied(snap->getMutableDataPtr(), diffsA);
    checkDiffsApplied(snap->getMutableDataPtr(), diffsB);
}

TEST_CASE_METHOD(SnapshotClientServerFixture,
                 "Test detailed snapshot diffs with merge ops",
                 "[snapshot]")
{
    // Set up a snapshot
    std::string snapKey = std::to_string(generateGid());
    int snapSize = 5 * HOST_PAGE_SIZE;
    auto snap = std::make_shared<SnapshotData>(snapSize);
    snap->makeRestorable(snapKey);
    reg.registerSnapshot(snapKey, snap);

    // Set up a couple of ints in the snapshot
    int offsetA1 = 8;
    int offsetA2 = 2 * HOST_PAGE_SIZE;
    int baseA1 = 25;
    int baseA2 = 60;

    int* basePtrA1 = (int*)(snap->getMutableDataPtr() + offsetA1);
    int* basePtrA2 = (int*)(snap->getMutableDataPtr() + offsetA2);
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
}

TEST_CASE_METHOD(SnapshotClientServerFixture,
                 "Test snapshot diffs with merge ops",
                 "[snapshot]")
{
    // Set up a snapshot
    std::string snapKey = std::to_string(generateGid());
    auto snap = setUpSnapshot(snapKey, 5, false);

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
    std::memcpy(snap->getMutableDataPtr(offset),
                originalData.data(),
                originalData.size());

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
    std::vector<uint8_t> actualData =
      snap->getDataCopy(offset, expectedData.size());
    REQUIRE(actualData == expectedData);
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
