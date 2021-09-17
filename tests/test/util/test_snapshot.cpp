#include <catch.hpp>

#include "faabric_utils.h"
#include "fixtures.h"

#include <faabric/util/bytes.h>
#include <faabric/util/macros.h>
#include <faabric/util/memory.h>
#include <faabric/util/snapshot.h>

using namespace faabric::util;

namespace tests {

TEST_CASE_METHOD(SnapshotTestFixture,
                 "Detailed test snapshot merge regions with ints",
                 "[util]")
{
    std::string snapKey = "foobar123";
    int snapPages = 5;

    int originalValueA = 100;
    int finalValueA = 150;
    int sumValueA = 50;

    int originalValueB = 300;
    int finalValueB = 425;
    int sumValueB = 125;

    faabric::util::SnapshotData snap;
    snap.size = snapPages * faabric::util::HOST_PAGE_SIZE;
    snap.data = allocatePages(snapPages);

    // Set up some integers in the snapshot
    int intAOffset = HOST_PAGE_SIZE + (10 * sizeof(int32_t));
    int intBOffset = (2 * HOST_PAGE_SIZE) + (20 * sizeof(int32_t));
    int* intAOriginal = (int*)(snap.data + intAOffset);
    int* intBOriginal = (int*)(snap.data + intBOffset);

    // Set the original values
    *intAOriginal = originalValueA;
    *intBOriginal = originalValueB;

    // Take the snapshot
    reg.takeSnapshot(snapKey, snap, true);

    // Map the snapshot to some memory
    size_t sharedMemSize = snapPages * HOST_PAGE_SIZE;
    uint8_t* sharedMem = allocatePages(snapPages);

    reg.mapSnapshot(snapKey, sharedMem);

    // Check mapping works
    int* intA = (int*)(sharedMem + intAOffset);
    int* intB = (int*)(sharedMem + intBOffset);

    REQUIRE(*intA == originalValueA);
    REQUIRE(*intB == originalValueB);

    // Reset dirty tracking to get a clean start
    faabric::util::resetDirtyTracking();

    // Set up the merge regions, deliberately do the one at higher offsets first
    // to check the ordering
    snap.addMergeRegion(intBOffset,
                        sizeof(int),
                        SnapshotDataType::Int,
                        SnapshotMergeOperation::Sum);

    snap.addMergeRegion(intAOffset,
                        sizeof(int),
                        SnapshotDataType::Int,
                        SnapshotMergeOperation::Sum);

    // Modify both values and some other data
    *intA = finalValueA;
    *intB = finalValueB;

    std::vector<uint8_t> otherData(100, 5);
    int otherOffset = (3 * HOST_PAGE_SIZE) + 5;
    std::memcpy(sharedMem + otherOffset, otherData.data(), otherData.size());

    // Get the snapshot diffs
    std::vector<SnapshotDiff> actualDiffs =
      snap.getChangeDiffs(sharedMem, sharedMemSize);

    // Check original hasn't changed
    REQUIRE(*intAOriginal == originalValueA);
    REQUIRE(*intBOriginal == originalValueB);

    // Check diffs themselves
    REQUIRE(actualDiffs.size() == 3);

    SnapshotDiff diffA = actualDiffs.at(0);
    SnapshotDiff diffB = actualDiffs.at(1);
    SnapshotDiff diffOther = actualDiffs.at(2);

    REQUIRE(diffA.offset == intAOffset);
    REQUIRE(diffB.offset == intBOffset);
    REQUIRE(diffOther.offset == otherOffset);

    REQUIRE(diffA.operation == SnapshotMergeOperation::Sum);
    REQUIRE(diffB.operation == SnapshotMergeOperation::Sum);
    REQUIRE(diffOther.operation == SnapshotMergeOperation::Overwrite);

    REQUIRE(diffA.dataType == SnapshotDataType::Int);
    REQUIRE(diffB.dataType == SnapshotDataType::Int);
    REQUIRE(diffOther.dataType == SnapshotDataType::Raw);

    REQUIRE(diffA.size == sizeof(int32_t));
    REQUIRE(diffB.size == sizeof(int32_t));
    REQUIRE(diffOther.size == otherData.size());

    // Check that original values have been subtracted from final values for
    // sums
    REQUIRE(*(int*)diffA.data == sumValueA);
    REQUIRE(*(int*)diffB.data == sumValueB);

    std::vector<uint8_t> actualOtherData(diffOther.data,
                                         diffOther.data + diffOther.size);
    REQUIRE(actualOtherData == otherData);

    deallocatePages(snap.data, snapPages);
}

TEST_CASE_METHOD(SnapshotTestFixture, "Test snapshot merge regions", "[util]")
{
    std::string snapKey = "foobar123";
    int snapPages = 5;

    int offset = HOST_PAGE_SIZE + (10 * sizeof(int32_t));

    faabric::util::SnapshotData snap;
    snap.size = snapPages * faabric::util::HOST_PAGE_SIZE;
    snap.data = allocatePages(snapPages);

    std::vector<uint8_t> originalData;
    std::vector<uint8_t> updatedData;
    std::vector<uint8_t> expectedData;

    faabric::util::SnapshotDataType dataType =
      faabric::util::SnapshotDataType::Raw;
    faabric::util::SnapshotMergeOperation operation =
      faabric::util::SnapshotMergeOperation::Overwrite;
    size_t dataLength = 0;

    SECTION("Integer")
    {
        int originalValue = 0;
        int finalValue = 0;
        int diffValue = 0;

        dataType = faabric::util::SnapshotDataType::Int;
        dataLength = sizeof(int32_t);

        SECTION("Integer sum")
        {
            originalValue = 100;
            finalValue = 150;
            diffValue = 50;

            operation = faabric::util::SnapshotMergeOperation::Sum;
        }

        SECTION("Integer subtract")
        {
            originalValue = 150;
            finalValue = 100;
            diffValue = 50;

            operation = faabric::util::SnapshotMergeOperation::Subtract;
        }

        SECTION("Integer product")
        {
            originalValue = 3;
            finalValue = 150;
            diffValue = 50;

            operation = faabric::util::SnapshotMergeOperation::Product;
        }

        SECTION("Integer max")
        {
            originalValue = 10;
            finalValue = 200;
            diffValue = 200;

            operation = faabric::util::SnapshotMergeOperation::Max;
        }

        SECTION("Integer min")
        {
            originalValue = 30;
            finalValue = 10;
            diffValue = 10;

            operation = faabric::util::SnapshotMergeOperation::Max;
        }

        originalData = faabric::util::valueToBytes<int>(originalValue);
        updatedData = faabric::util::valueToBytes<int>(finalValue);
        expectedData = faabric::util::valueToBytes<int>(diffValue);
    }

    // Write the original data into place
    std::memcpy(snap.data + offset, originalData.data(), originalData.size());

    // Take the snapshot
    reg.takeSnapshot(snapKey, snap, true);

    // Map the snapshot to some memory
    size_t sharedMemSize = snapPages * HOST_PAGE_SIZE;
    uint8_t* sharedMem = allocatePages(snapPages);

    reg.mapSnapshot(snapKey, sharedMem);

    // Reset dirty tracking
    faabric::util::resetDirtyTracking();

    // Set up the merge region
    snap.addMergeRegion(offset, dataLength, dataType, operation);

    // Modify the value
    std::memcpy(sharedMem + offset, updatedData.data(), updatedData.size());

    // Get the snapshot diffs
    std::vector<SnapshotDiff> actualDiffs =
      snap.getChangeDiffs(sharedMem, sharedMemSize);

    // Check number of diffs
    REQUIRE(actualDiffs.size() == 1);

    SnapshotDiff diff = actualDiffs.at(0);
    REQUIRE(diff.offset == offset);
    REQUIRE(diff.operation == operation);
    REQUIRE(diff.dataType == dataType);
    REQUIRE(diff.size == dataLength);

    // Check actual and expected
    std::vector<uint8_t> actualData(diff.data, diff.data + dataLength);
    REQUIRE(actualData == expectedData);

    deallocatePages(snap.data, snapPages);
}

TEST_CASE_METHOD(SnapshotTestFixture, "Test invalid snapshot merges", "[util]")
{
    std::string snapKey = "foobar123";
    int snapPages = 3;
    int offset = HOST_PAGE_SIZE + (2 * sizeof(int32_t));

    faabric::util::SnapshotData snap;
    snap.size = snapPages * faabric::util::HOST_PAGE_SIZE;
    snap.data = allocatePages(snapPages);

    faabric::util::SnapshotDataType dataType =
      faabric::util::SnapshotDataType::Raw;
    faabric::util::SnapshotMergeOperation operation =
      faabric::util::SnapshotMergeOperation::Overwrite;
    size_t dataLength = 0;

    std::string expectedMsg;

    SECTION("Integer overwrite")
    {
        dataType = faabric::util::SnapshotDataType::Int;
        dataLength = sizeof(int32_t);
        expectedMsg = "Unhandled integer merge operation";
    }

    SECTION("Raw sum")
    {
        dataType = faabric::util::SnapshotDataType::Raw;
        dataLength = 123;
        expectedMsg = "Merge region for unhandled data type";
    }

    // Take the snapshot
    reg.takeSnapshot(snapKey, snap, true);

    // Map the snapshot
    size_t sharedMemSize = snapPages * HOST_PAGE_SIZE;
    uint8_t* sharedMem = allocatePages(snapPages);
    reg.mapSnapshot(snapKey, sharedMem);

    // Reset dirty tracking
    faabric::util::resetDirtyTracking();

    // Set up the merge region
    snap.addMergeRegion(offset, dataLength, dataType, operation);

    // Modify the value
    std::vector<uint8_t> bytes(dataLength, 3);
    std::memcpy(sharedMem + offset, bytes.data(), bytes.size());

    // Check getting diffs throws an exception
    bool failed = false;
    try {
        snap.getChangeDiffs(sharedMem, sharedMemSize);
    } catch (std::runtime_error& ex) {
        failed = true;
        REQUIRE(ex.what() == expectedMsg);
    }

    REQUIRE(failed);
    deallocatePages(snap.data, snapPages);
}
}
