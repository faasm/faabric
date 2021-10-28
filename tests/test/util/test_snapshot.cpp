#include <catch2/catch.hpp>

#include "faabric_utils.h"
#include "fixtures.h"

#include <faabric/util/bytes.h>
#include <faabric/util/macros.h>
#include <faabric/util/memory.h>
#include <faabric/util/snapshot.h>

using namespace faabric::util;

namespace tests {

class SnapshotMergeTestFixture : public SnapshotTestFixture
{
  public:
    SnapshotMergeTestFixture() {}
    ~SnapshotMergeTestFixture() {}

  protected:
    std::string snapKey;
    int snapPages;
    faabric::util::SnapshotData snap;

    uint8_t* setUpSnapshot(int snapPages)
    {
        snapKey = "foobar123";
        snap.size = snapPages * faabric::util::HOST_PAGE_SIZE;
        snap.data = allocatePages(snapPages);

        // Take the snapshot
        reg.takeSnapshot(snapKey, snap, true);

        // Map the snapshot
        uint8_t* sharedMem = allocatePages(snapPages);
        reg.mapSnapshot(snapKey, sharedMem);

        // Reset dirty tracking
        faabric::util::resetDirtyTracking();

        return sharedMem;
    }

    void checkDiffs(std::vector<SnapshotDiff>& actualDiffs,
                    std::vector<SnapshotDiff>& expectedDiffs)
    {
        REQUIRE(actualDiffs.size() == expectedDiffs.size());

        for (int i = 0; i < actualDiffs.size(); i++) {
            SnapshotDiff actualDiff = actualDiffs.at(i);
            SnapshotDiff expectedDiff = expectedDiffs.at(i);

            REQUIRE(actualDiff.operation == expectedDiff.operation);
            REQUIRE(actualDiff.dataType == expectedDiff.dataType);
            REQUIRE(actualDiff.offset == expectedDiff.offset);

            std::vector<uint8_t> actualData(actualDiff.data,
                                            actualDiff.data + actualDiff.size);
            std::vector<uint8_t> expectedData(
              expectedDiff.data, expectedDiff.data + expectedDiff.size);
            REQUIRE(actualData == expectedData);
        }
    }
};

TEST_CASE_METHOD(SnapshotMergeTestFixture,
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

TEST_CASE_METHOD(SnapshotMergeTestFixture,
                 "Test edge-cases of snapshot merge regions",
                 "[util]")
{
    // Region edge cases:
    // - start
    // - adjacent
    // - finish

    std::string snapKey = "foobar123";
    int snapPages = 5;
    int snapSize = snapPages * faabric::util::HOST_PAGE_SIZE;

    int originalA = 50;
    int finalA = 25;
    int subA = 25;
    uint32_t offsetA = 0;

    int originalB = 100;
    int finalB = 200;
    int sumB = 100;
    uint32_t offsetB = HOST_PAGE_SIZE + (2 * sizeof(int32_t));

    int originalC = 200;
    int finalC = 150;
    int subC = 50;
    uint32_t offsetC = offsetB + sizeof(int32_t);

    int originalD = 100;
    int finalD = 150;
    int sumD = 50;
    uint32_t offsetD = snapSize - sizeof(int32_t);

    faabric::util::SnapshotData snap;
    snap.size = snapSize;
    snap.data = allocatePages(snapPages);

    // Set up original values
    *(int*)(snap.data + offsetA) = originalA;
    *(int*)(snap.data + offsetB) = originalB;
    *(int*)(snap.data + offsetC) = originalC;
    *(int*)(snap.data + offsetD) = originalD;

    // Take the snapshot
    reg.takeSnapshot(snapKey, snap, true);

    // Map the snapshot to some memory
    size_t sharedMemSize = snapPages * HOST_PAGE_SIZE;
    uint8_t* sharedMem = allocatePages(snapPages);

    reg.mapSnapshot(snapKey, sharedMem);

    // Reset dirty tracking
    faabric::util::resetDirtyTracking();

    // Set up the merge regions
    snap.addMergeRegion(offsetA,
                        sizeof(int),
                        SnapshotDataType::Int,
                        SnapshotMergeOperation::Subtract);

    snap.addMergeRegion(
      offsetB, sizeof(int), SnapshotDataType::Int, SnapshotMergeOperation::Sum);

    snap.addMergeRegion(offsetC,
                        sizeof(int),
                        SnapshotDataType::Int,
                        SnapshotMergeOperation::Subtract);

    snap.addMergeRegion(
      offsetD, sizeof(int), SnapshotDataType::Int, SnapshotMergeOperation::Sum);

    // Set final values
    *(int*)(sharedMem + offsetA) = finalA;
    *(int*)(sharedMem + offsetB) = finalB;
    *(int*)(sharedMem + offsetC) = finalC;
    *(int*)(sharedMem + offsetD) = finalD;

    // Check the diffs
    std::vector<SnapshotDiff> expectedDiffs = {
        { SnapshotDataType::Int,
          SnapshotMergeOperation::Subtract,
          offsetA,
          BYTES(&subA),
          sizeof(int32_t) },
        { SnapshotDataType::Int,
          SnapshotMergeOperation::Sum,
          offsetB,
          BYTES(&sumB),
          sizeof(int32_t) },
        { SnapshotDataType::Int,
          SnapshotMergeOperation::Subtract,
          offsetC,
          BYTES(&subC),
          sizeof(int32_t) },
        { SnapshotDataType::Int,
          SnapshotMergeOperation::Sum,
          offsetD,
          BYTES(&sumD),
          sizeof(int32_t) },
    };

    std::vector<SnapshotDiff> actualDiffs =
      snap.getChangeDiffs(sharedMem, sharedMemSize);
    REQUIRE(actualDiffs.size() == 4);

    checkDiffs(actualDiffs, expectedDiffs);
}

TEST_CASE_METHOD(SnapshotMergeTestFixture,
                 "Test snapshot merge regions",
                 "[util]")
{
    std::string snapKey = "foobar123";
    int snapPages = 5;

    uint32_t offset = HOST_PAGE_SIZE + (10 * sizeof(int32_t));

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

    int expectedNumDiffs = 1;

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

    SECTION("Raw")
    {
        dataLength = 2 * sizeof(int32_t);
        originalData = std::vector<uint8_t>(dataLength, 3);
        updatedData = originalData;
        expectedData = originalData;

        dataType = faabric::util::SnapshotDataType::Raw;
        operation = faabric::util::SnapshotMergeOperation::Overwrite;

        SECTION("Ignore")
        {
            operation = faabric::util::SnapshotMergeOperation::Ignore;

            // Scatter some modifications through the updated data, to make sure
            // none are picked up
            updatedData[0] = 1;
            updatedData[sizeof(int32_t) - 2] = 1;
            updatedData[sizeof(int32_t) + 10] = 1;

            expectedNumDiffs = 0;
        }
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
    REQUIRE(actualDiffs.size() == expectedNumDiffs);

    if (expectedNumDiffs == 1) {
        std::vector<SnapshotDiff> expectedDiffs = { { dataType,
                                                      operation,
                                                      offset,
                                                      expectedData.data(),
                                                      expectedData.size() } };

        checkDiffs(actualDiffs, expectedDiffs);
    }

    deallocatePages(snap.data, snapPages);
}

TEST_CASE_METHOD(SnapshotMergeTestFixture,
                 "Test invalid snapshot merges",
                 "[util]")
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
        operation = faabric::util::SnapshotMergeOperation::Overwrite;
        dataLength = sizeof(int32_t);
        expectedMsg = "Unhandled integer merge operation";
    }

    SECTION("Raw sum")
    {
        dataType = faabric::util::SnapshotDataType::Raw;
        operation = faabric::util::SnapshotMergeOperation::Sum;
        dataLength = 123;
        expectedMsg = "Unhandled raw merge operation";
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

TEST_CASE_METHOD(SnapshotMergeTestFixture, "Test cross-page ignores", "[util]")
{
    int snapPages = 6;
    size_t sharedMemSize = snapPages * HOST_PAGE_SIZE;
    uint8_t* sharedMem = setUpSnapshot(snapPages);

    // Add ignore regions that cover multiple pages
    int ignoreOffsetA = HOST_PAGE_SIZE + 100;
    int ignoreLengthA = 2 * HOST_PAGE_SIZE;
    snap.addMergeRegion(ignoreOffsetA,
                        ignoreLengthA,
                        faabric::util::SnapshotDataType::Raw,
                        faabric::util::SnapshotMergeOperation::Ignore);

    int ignoreOffsetB = sharedMemSize - (HOST_PAGE_SIZE + 10);
    int ignoreLengthB = 30;
    snap.addMergeRegion(ignoreOffsetB,
                        ignoreLengthB,
                        faabric::util::SnapshotDataType::Raw,
                        faabric::util::SnapshotMergeOperation::Ignore);

    // Add some modifications that will cause diffs, and some should be ignored
    std::vector<uint8_t> dataA(10, 1);
    std::vector<uint8_t> dataB(1, 1);
    std::vector<uint8_t> dataC(1, 1);
    std::vector<uint8_t> dataD(3, 1);

    // Not ignored, start of memory
    uint32_t offsetA = 0;
    std::memcpy(sharedMem + offsetA, dataA.data(), dataA.size());

    // Not ignored, just before first ignore region
    uint32_t offsetB = ignoreOffsetA - 1;
    std::memcpy(sharedMem + offsetB, dataB.data(), dataB.size());

    // Not ignored, just after first ignore region
    uint32_t offsetC = ignoreOffsetA + ignoreLengthA;
    std::memcpy(sharedMem + offsetC, dataC.data(), dataC.size());

    // Not ignored, just before second ignore region
    uint32_t offsetD = ignoreOffsetB - 4;
    std::memcpy(sharedMem + offsetD, dataD.data(), dataD.size());

    // Ignored, start of first ignore region
    sharedMem[ignoreOffsetA] = (uint8_t)1;

    // Ignored, just before page boundary in ignore region
    sharedMem[(2 * HOST_PAGE_SIZE) - 1] = (uint8_t)1;

    // Ignored, just after page boundary in ignore region
    sharedMem[(2 * HOST_PAGE_SIZE)] = (uint8_t)1;

    // Deliberately don't put any changes after the next page boundary to check
    // that it rolls over properly

    // Ignored, just inside second region
    sharedMem[ignoreOffsetB + 2] = (uint8_t)1;

    // Ignored, end of second region
    sharedMem[ignoreOffsetB + ignoreLengthB - 1] = (uint8_t)1;

    std::vector<SnapshotDiff> expectedDiffs = {
        { offsetA, dataA.data(), dataA.size() },
        { offsetB, dataB.data(), dataB.size() },
        { offsetC, dataC.data(), dataC.size() },
        { offsetD, dataD.data(), dataD.size() },
    };

    // Check number of diffs
    std::vector<SnapshotDiff> actualDiffs =
      snap.getChangeDiffs(sharedMem, sharedMemSize);

    checkDiffs(actualDiffs, expectedDiffs);
}

TEST_CASE_METHOD(SnapshotMergeTestFixture,
                 "Test fine-grained byte-wise diffs",
                 "[util]")
{
    int snapPages = 3;
    size_t sharedMemSize = snapPages * HOST_PAGE_SIZE;
    uint8_t* sharedMem = setUpSnapshot(snapPages);

    // Add some tightly-packed changes
    uint32_t offsetA = 0;
    std::vector<uint8_t> dataA(10, 1);
    std::memcpy(sharedMem + offsetA, dataA.data(), dataA.size());

    uint32_t offsetB = dataA.size() + 1;
    std::vector<uint8_t> dataB(2, 1);
    std::memcpy(sharedMem + offsetB, dataB.data(), dataB.size());

    uint32_t offsetC = offsetB + 3;
    std::vector<uint8_t> dataC(1, 1);
    std::memcpy(sharedMem + offsetC, dataC.data(), dataC.size());

    uint32_t offsetD = offsetC + 2;
    std::vector<uint8_t> dataD(1, 1);
    std::memcpy(sharedMem + offsetD, dataD.data(), dataD.size());

    std::vector<SnapshotDiff> expectedDiffs = {
        { offsetA, dataA.data(), dataA.size() },
        { offsetB, dataB.data(), dataB.size() },
        { offsetC, dataC.data(), dataC.size() },
        { offsetD, dataD.data(), dataD.size() },
    };

    // Check number of diffs
    std::vector<SnapshotDiff> actualDiffs =
      snap.getChangeDiffs(sharedMem, sharedMemSize);

    checkDiffs(actualDiffs, expectedDiffs);
}

TEST_CASE_METHOD(SnapshotMergeTestFixture,
                 "Test mix of applicable and non-applicable merge regions",
                 "[util]")
{
    int snapPages = 6;
    size_t sharedMemSize = snapPages * HOST_PAGE_SIZE;
    uint8_t* sharedMem = setUpSnapshot(snapPages);

    // Add a couple of merge regions on each page, which should be skipped as
    // they won't cover any changes
    for (int i = 0; i < snapPages; i++) {
        // Ignore
        int iOff = i * HOST_PAGE_SIZE;
        snap.addMergeRegion(iOff,
                            10,
                            faabric::util::SnapshotDataType::Raw,
                            faabric::util::SnapshotMergeOperation::Ignore);

        // Sum
        int sOff = ((i + 1) * HOST_PAGE_SIZE) - (2 * sizeof(int32_t));
        snap.addMergeRegion(sOff,
                            sizeof(int32_t),
                            faabric::util::SnapshotDataType::Int,
                            faabric::util::SnapshotMergeOperation::Sum);
    }

    // Add an ignore region that should take effect, along with a corresponding
    // change to be ignored
    uint32_t ignoreA = (2 * HOST_PAGE_SIZE) + 2;
    snap.addMergeRegion(ignoreA,
                        20,
                        faabric::util::SnapshotDataType::Raw,
                        faabric::util::SnapshotMergeOperation::Ignore);
    std::vector<uint8_t> dataA(10, 1);
    std::memcpy(sharedMem + ignoreA, dataA.data(), dataA.size());

    // Add a sum region and data that should also take effect
    uint32_t sumOffset = (4 * HOST_PAGE_SIZE) + 100;
    int sumValue = 333;
    int sumOriginal = 111;
    int sumExpected = 222;
    snap.addMergeRegion(sumOffset,
                        sizeof(int32_t),
                        faabric::util::SnapshotDataType::Int,
                        faabric::util::SnapshotMergeOperation::Sum);
    *(int*)(snap.data + sumOffset) = sumOriginal;
    *(int*)(sharedMem + sumOffset) = sumValue;

    // Check diffs
    std::vector<SnapshotDiff> expectedDiffs = {
        { faabric::util::SnapshotDataType::Int,
          faabric::util::SnapshotMergeOperation::Sum,
          sumOffset,
          BYTES(&sumExpected),
          sizeof(int32_t) },
    };

    std::vector<SnapshotDiff> actualDiffs =
      snap.getChangeDiffs(sharedMem, sharedMemSize);

    checkDiffs(actualDiffs, expectedDiffs);
}
}
