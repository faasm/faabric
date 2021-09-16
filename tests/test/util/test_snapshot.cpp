#include <catch.hpp>

#include "faabric_utils.h"
#include "fixtures.h"

#include <faabric/util/memory.h>
#include <faabric/util/snapshot.h>

using namespace faabric::util;

namespace tests {

TEST_CASE_METHOD(SnapshotTestFixture, "Test snapshot merge regions", "[util]")
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

    // Set up the merge regions
    snap.addMergeRegion(intAOffset,
                        sizeof(int),
                        SnapshotDataType::Int,
                        SnapshotMergeOperation::Sum);

    snap.addMergeRegion(intBOffset,
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
}
}
