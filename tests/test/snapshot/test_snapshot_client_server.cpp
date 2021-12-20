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

    // Set up snapshots
    auto snapA = std::make_shared<SnapshotData>(dataA);
    auto snapB = std::make_shared<SnapshotData>(dataB);

    // Add merge regions to one
    std::vector<SnapshotMergeRegion> mergeRegions = {
        { 123, 1234, SnapshotDataType::Int, SnapshotMergeOperation::Sum },
        { 345, 3456, SnapshotDataType::Raw, SnapshotMergeOperation::Overwrite }
    };

    for (const auto& m : mergeRegions) {
        snapA->addMergeRegion(m.offset, m.length, m.dataType, m.operation);
    }

    REQUIRE(reg.getSnapshotCount() == 0);

    // Send the messages
    cli.pushSnapshot(snapKeyA, snapA);
    cli.pushSnapshot(snapKeyB, snapB);

    // Check snapshots created in registry
    REQUIRE(reg.getSnapshotCount() == 2);
    const auto actualA = reg.getSnapshot(snapKeyA);
    const auto actualB = reg.getSnapshot(snapKeyB);

    REQUIRE(actualA->getSize() == snapA->getSize());
    REQUIRE(actualB->getSize() == snapB->getSize());

    // Check merge regions
    REQUIRE(actualA->getMergeRegions().size() == mergeRegions.size());
    REQUIRE(actualB->getMergeRegions().empty());

    for (int i = 0; i < mergeRegions.size(); i++) {
        SnapshotMergeRegion expected = mergeRegions.at(i);
        SnapshotMergeRegion actual = snapA->getMergeRegions()[expected.offset];

        REQUIRE(actual.offset == expected.offset);
        REQUIRE(actual.dataType == expected.dataType);
        REQUIRE(actual.length == expected.length);
        REQUIRE(actual.operation == expected.operation);
    }

    // Check data contents
    std::vector<uint8_t> actualDataA = actualA->getDataCopy();
    std::vector<uint8_t> actualDataB = actualB->getDataCopy();

    REQUIRE(actualDataA == dataA);
    REQUIRE(actualDataB == dataB);
}

void checkDiffsApplied(const uint8_t* snapBase, std::vector<SnapshotDiff> diffs)
{
    for (auto& d : diffs) {
        std::vector<uint8_t> actual(snapBase + d.getOffset(),
                                    snapBase + d.getOffset() +
                                      d.getData().size());

        std::vector<uint8_t> expected(d.getData().begin(), d.getData().end());

        REQUIRE(actual == expected);
    }
}

TEST_CASE_METHOD(SnapshotClientServerFixture,
                 "Test push snapshot diffs",
                 "[snapshot]")
{
    std::string thisHost = getSystemConfig().endpointHost;

    // Set up a snapshot that's got enough memory to expand into
    std::string snapKey = std::to_string(generateGid());
    size_t initialSnapSize = 5 * HOST_PAGE_SIZE;
    size_t expandedSnapSize = 10 * HOST_PAGE_SIZE;

    auto snap =
      std::make_shared<SnapshotData>(initialSnapSize, expandedSnapSize);

    // Set up the snapshot
    reg.registerSnapshot(snapKey, snap);

    // Set up another snapshot with some merge regions to check they're added
    // on an update
    auto otherSnap =
      std::make_shared<SnapshotData>(initialSnapSize, expandedSnapSize);

    std::vector<SnapshotMergeRegion> mergeRegions = {
        { 123, 1234, SnapshotDataType::Int, SnapshotMergeOperation::Sum },
        { 345, 3456, SnapshotDataType::Raw, SnapshotMergeOperation::Overwrite }
    };

    for (const auto& m : mergeRegions) {
        otherSnap->addMergeRegion(m.offset, m.length, m.dataType, m.operation);
    }

    // Set up some diffs for the initial request
    uint32_t offsetA1 = 5;
    uint32_t offsetA2 = 2 * HOST_PAGE_SIZE;
    std::vector<uint8_t> diffDataA1 = { 0, 1, 2, 3 };
    std::vector<uint8_t> diffDataA2 = { 4, 5, 6 };

    REQUIRE(snap->getQueuedDiffsCount() == 0);

    SnapshotDiff diffA1(SnapshotDataType::Raw,
                        SnapshotMergeOperation::Overwrite,
                        offsetA1,
                        diffDataA1);

    SnapshotDiff diffA2(SnapshotDataType::Raw,
                        SnapshotMergeOperation::Overwrite,
                        offsetA2,
                        diffDataA2);

    std::vector<SnapshotDiff> diffsA = { diffA1, diffA2 };
    cli.pushSnapshotDiffs(snapKey, diffsA);
    REQUIRE(snap->getQueuedDiffsCount() == 2);

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
                        diffDataB1);

    SnapshotDiff diffB2(SnapshotDataType::Raw,
                        SnapshotMergeOperation::Overwrite,
                        offsetB2,
                        diffDataB2);

    SnapshotDiff diffB3(SnapshotDataType::Raw,
                        SnapshotMergeOperation::Overwrite,
                        offsetB3,
                        diffDataB3);

    std::vector<SnapshotDiff> diffsB = { diffB1, diffB2, diffB3 };

    SECTION("Full update")
    {
        // Make the request
        cli.pushSnapshotUpdate(snapKey, otherSnap, diffsB);

        // Check nothing queued
        REQUIRE(snap->getQueuedDiffsCount() == 0);

        // Check merge regions from other snap pushed
        REQUIRE(snap->getMergeRegions().size() == mergeRegions.size());

        for (int i = 0; i < mergeRegions.size(); i++) {
            SnapshotMergeRegion expected = mergeRegions.at(i);
            SnapshotMergeRegion actual =
              snap->getMergeRegions()[expected.offset];

            REQUIRE(actual.offset == expected.offset);
            REQUIRE(actual.dataType == expected.dataType);
            REQUIRE(actual.length == expected.length);
            REQUIRE(actual.operation == expected.operation);
        }
    }

    SECTION("Just diffs")
    {
        // Make the request
        cli.pushSnapshotDiffs(snapKey, diffsB);

        // Check and write queued diffs
        REQUIRE(snap->getQueuedDiffsCount() == 5);

        snap->writeQueuedDiffs();

        // Check no merge regions sent
        REQUIRE(snap->getMergeRegions().empty());
    }

    // Check diffs have been applied
    checkDiffsApplied(snap->getDataPtr(), diffsA);
    checkDiffsApplied(snap->getDataPtr(), diffsB);
}

TEST_CASE_METHOD(SnapshotClientServerFixture,
                 "Test detailed snapshot diffs with merge ops",
                 "[snapshot]")
{
    // Set up a snapshot
    std::string snapKey = std::to_string(generateGid());
    int snapSize = 5 * HOST_PAGE_SIZE;
    auto snap = std::make_shared<SnapshotData>(snapSize);
    reg.registerSnapshot(snapKey, snap);

    // Set up a couple of ints in the snapshot
    int offsetA1 = 8;
    int offsetA2 = 2 * HOST_PAGE_SIZE;
    int baseA1 = 25;
    int baseA2 = 60;

    snap->copyInData({ BYTES(&baseA1), sizeof(int) }, offsetA1);
    snap->copyInData({ BYTES(&baseA2), sizeof(int) }, offsetA2);

    // Set up some diffs with different merge operations
    int diffIntA1 = 123;
    int diffIntA2 = 345;

    std::vector<uint8_t> intDataA1 = valueToBytes<int>(diffIntA1);
    std::vector<uint8_t> intDataA2 = valueToBytes<int>(diffIntA2);

    std::vector<SnapshotDiff> diffs;

    SnapshotDiff diffA1(
      SnapshotDataType::Int, SnapshotMergeOperation::Sum, offsetA1, intDataA1);

    SnapshotDiff diffA2(
      SnapshotDataType::Int, SnapshotMergeOperation::Sum, offsetA2, intDataA2);

    size_t originalDiffsApplied = snap->getQueuedDiffsCount();

    diffs = { diffA1, diffA2 };
    cli.pushSnapshotDiffs(snapKey, diffs);

    // Ensure the right number of diffs is applied
    REQUIRE(snap->getQueuedDiffsCount() == originalDiffsApplied + 2);

    // Write and check diffs have been applied according to the merge operations
    snap->writeQueuedDiffs();
    const uint8_t* rawSnapData = snap->getDataPtr();
    int actualA1 = faabric::util::unalignedRead<int>(rawSnapData + offsetA1);
    int actualA2 = faabric::util::unalignedRead<int>(rawSnapData + offsetA2);
    REQUIRE(actualA1 == baseA1 + diffIntA1);
    REQUIRE(actualA2 == baseA2 + diffIntA2);
}

TEST_CASE_METHOD(SnapshotClientServerFixture,
                 "Test snapshot diffs with merge ops",
                 "[snapshot]")
{
    // Set up a snapshot
    std::string snapKey = std::to_string(generateGid());
    auto snap = setUpSnapshot(snapKey, 5);

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
    snap->copyInData(originalData, offset);

    SnapshotDiff diff(dataType, operation, offset, diffData);

    size_t originalDiffsApplied = snap->getQueuedDiffsCount();

    std::vector<SnapshotDiff> diffs = { diff };
    cli.pushSnapshotDiffs(snapKey, diffs);

    // Ensure the right number of diffs is applied
    REQUIRE(snap->getQueuedDiffsCount() == originalDiffsApplied + 1);

    // Apply and check data is as expected
    snap->writeQueuedDiffs();
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
