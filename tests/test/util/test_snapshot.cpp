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
    SnapshotMergeTestFixture() = default;

    ~SnapshotMergeTestFixture() = default;

  protected:
    std::string snapKey = "foobar123";

    std::shared_ptr<SnapshotData> setUpSnapshot(int snapPages,
                                                int sharedMemPages)

    {
        auto snapData =
          std::make_shared<SnapshotData>(snapPages * HOST_PAGE_SIZE);

        // Take the snapshot
        snapData->makeRestorable(snapKey);
        reg.registerSnapshot(snapKey, snapData);

        return snapData;
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
                 "Test clear merge regions",
                 "[snapshot][util]")
{
    int snapPages = 20;
    setUpSnapshot(snapPages, snapPages);
    auto snap = reg.getSnapshot(snapKey);

    int nRegions = 10;
    for (int i = 0; i < nRegions; i++) {
        snap->addMergeRegion(i * HOST_PAGE_SIZE,
                             sizeof(int),
                             SnapshotDataType::Int,
                             SnapshotMergeOperation::Sum);
    }

    REQUIRE(snap->getMergeRegions().size() == nRegions);

    snap->clearMergeRegions();

    REQUIRE(snap->getMergeRegions().empty());
}

TEST_CASE_METHOD(SnapshotMergeTestFixture,
                 "Test mapping snapshot memory",
                 "[snapshot][util]")
{
    int snapPages = 10;
    auto snap = std::make_shared<SnapshotData>(snapPages * HOST_PAGE_SIZE);

    // Put some data into the snapshot
    std::vector<uint8_t> dataA(100, 2);
    std::vector<uint8_t> dataB(300, 4);

    uint8_t* snapData = snap->getMutableDataPtr();
    std::memcpy(snapData + (2 * HOST_PAGE_SIZE), dataA.data(), dataA.size());
    std::memcpy(
      snapData + (5 * HOST_PAGE_SIZE) + 2, dataB.data(), dataB.size());

    // Record the snap memory
    std::vector<uint8_t> actualSnapMem = snap->getDataCopy();

    // Make it restorable
    REQUIRE(!snap->isRestorable());
    snap->makeRestorable("snap-map-test");
    REQUIRE(snap->isRestorable());

    // Set up shared memory
    int sharedMemPages = 20;
    MemoryRegion sharedMem =
      allocateSharedMemory(sharedMemPages * HOST_PAGE_SIZE);

    // Check it's zeroed
    std::vector<uint8_t> expectedInitial(snap->size, 0);
    std::vector<uint8_t> actualSharedMemBefore(sharedMem.get(),
                                               sharedMem.get() + snap->size);
    REQUIRE(actualSharedMemBefore == expectedInitial);

    // Map the snapshot and check again
    snap->mapToMemory(sharedMem.get());
    std::vector<uint8_t> actualSharedMemAfter(sharedMem.get(),
                                              sharedMem.get() + snap->size);
    REQUIRE(actualSharedMemAfter == actualSnapMem);
}

TEST_CASE_METHOD(SnapshotMergeTestFixture,
                 "Test growing snapshots",
                 "[snapshot][util]")
{
    // Set up the snapshot
    int originalPages = 10;
    size_t originalSize = originalPages * HOST_PAGE_SIZE;
    int expandedPages = 20;
    size_t expandedSize = expandedPages * HOST_PAGE_SIZE;

    std::shared_ptr<SnapshotData> snap =
      std::make_shared<SnapshotData>(originalSize, expandedSize);

    // Take the snapsho at the original size
    reg.registerSnapshot(snapKey, snap);

    // Put some data into the snapshot
    std::vector<uint8_t> dataA(100, 2);
    std::vector<uint8_t> dataB(300, 4);

    snap->copyInData(dataA, 2 * HOST_PAGE_SIZE);
    snap->copyInData(dataB, (5 * HOST_PAGE_SIZE) + 2);

    std::vector<uint8_t> originalData = snap->getDataCopy();
    REQUIRE(originalData.size() == originalSize);

    // Make it restorable
    std::string fdLabel = "snap-map-test";
    REQUIRE(!snap->isRestorable());
    snap->makeRestorable(fdLabel);
    REQUIRE(snap->isRestorable());

    // Map to some other region of memory large enough for the extended version
    MemoryRegion sharedMem = allocateSharedMemory(expandedSize);
    snap->mapToMemory(sharedMem.get());

    // Add some data to the extended region. Check the snapshot extends to fit
    std::vector<uint8_t> dataC(300, 5);
    std::vector<uint8_t> dataD(200, 6);
    uint32_t extendedOffsetA = (originalPages + 3) * HOST_PAGE_SIZE;
    uint32_t extendedOffsetB = (originalPages + 5) * HOST_PAGE_SIZE;

    snap->copyInData(dataC, extendedOffsetA);
    size_t expectedSizeA = extendedOffsetA + dataC.size();
    REQUIRE(snap->size == expectedSizeA);

    snap->copyInData(dataD, extendedOffsetB);
    size_t expectedSizeB = extendedOffsetB + dataD.size();
    REQUIRE(snap->size == expectedSizeB);

    // Remap to shared memory
    snap->mapToMemory(sharedMem.get());

    // Check mapped region matches
    std::vector<uint8_t> actualData = snap->getDataCopy();
    std::vector<uint8_t> actualSharedMem(sharedMem.get(),
                                         sharedMem.get() + snap->size);

    REQUIRE(actualSharedMem.size() == actualData.size());
    REQUIRE(actualSharedMem == actualData);
}

TEST_CASE_METHOD(SnapshotMergeTestFixture,
                 "Detailed test snapshot merge regions with ints",
                 "[snapshot][util]")
{
    std::string snapKey = "foobar123";
    int snapPages = 5;

    int originalValueA = 123456;
    int finalValueA = 150000;
    int sumValueA = 26544;

    int originalValueB = 300000;
    int finalValueB = 650123;
    int sumValueB = 350123;

    size_t memSize = snapPages * HOST_PAGE_SIZE;
    auto snap = std::make_shared<SnapshotData>(memSize);
    uint8_t* snapData = snap->getMutableDataPtr();

    // Set up some integers in the snapshot
    int intAOffset = HOST_PAGE_SIZE + (10 * sizeof(int32_t));
    int intBOffset = (2 * HOST_PAGE_SIZE) + (20 * sizeof(int32_t));
    int* intAOriginal = (int*)(snapData + intAOffset);
    int* intBOriginal = (int*)(snapData + intBOffset);

    // Set the original values
    *intAOriginal = originalValueA;
    *intBOriginal = originalValueB;

    // Take the snapshot
    snap->makeRestorable(snapKey);
    reg.registerSnapshot(snapKey, snap);

    // Map the snapshot to some memory
    MemoryRegion sharedMem = allocateSharedMemory(memSize);

    reg.mapSnapshot(snapKey, sharedMem.get());

    // Check mapping works
    int* intA = (int*)(sharedMem.get() + intAOffset);
    int* intB = (int*)(sharedMem.get() + intBOffset);

    REQUIRE(*intA == originalValueA);
    REQUIRE(*intB == originalValueB);

    // Reset dirty tracking to get a clean start
    faabric::util::resetDirtyTracking();

    // Set up the merge regions, deliberately do the one at higher offsets first
    // to check the ordering
    snap->addMergeRegion(intBOffset,
                         sizeof(int),
                         SnapshotDataType::Int,
                         SnapshotMergeOperation::Sum);

    snap->addMergeRegion(intAOffset,
                         sizeof(int),
                         SnapshotDataType::Int,
                         SnapshotMergeOperation::Sum);

    // Modify both values and some other data
    *intA = finalValueA;
    *intB = finalValueB;

    std::vector<uint8_t> otherData(100, 5);
    int otherOffset = (3 * HOST_PAGE_SIZE) + 5;
    std::memcpy(
      sharedMem.get() + otherOffset, otherData.data(), otherData.size());

    // Get the snapshot diffs
    std::vector<SnapshotDiff> actualDiffs =
      snap->getChangeDiffs({ sharedMem.get(), memSize });

    // Check original hasn't changed
    REQUIRE(*intAOriginal == originalValueA);
    REQUIRE(*intBOriginal == originalValueB);

    // Check diffs themselves
    REQUIRE(actualDiffs.size() == 2);

    SnapshotDiff diffA = actualDiffs.at(0);
    SnapshotDiff diffB = actualDiffs.at(1);

    REQUIRE(diffA.offset == intAOffset);
    REQUIRE(diffB.offset == intBOffset);

    REQUIRE(diffA.operation == SnapshotMergeOperation::Sum);
    REQUIRE(diffB.operation == SnapshotMergeOperation::Sum);

    REQUIRE(diffA.dataType == SnapshotDataType::Int);
    REQUIRE(diffB.dataType == SnapshotDataType::Int);

    REQUIRE(diffA.size == sizeof(int32_t));
    REQUIRE(diffB.size == sizeof(int32_t));

    // Check that original values have been subtracted from final values for
    // sums
    REQUIRE(*(int*)diffA.data == sumValueA);
    REQUIRE(*(int*)diffB.data == sumValueB);
}

TEST_CASE_METHOD(SnapshotMergeTestFixture,
                 "Test edge-cases of snapshot merge regions",
                 "[snapshot][util]")
{
    // Region edge cases:
    // - start
    // - adjacent
    // - finish

    // Note here that we want to make sure we're checking larger integers to
    // exercise a change in all bytes of the integer.
    std::string snapKey = "foobar123";
    int snapPages = 5;
    int snapSize = snapPages * faabric::util::HOST_PAGE_SIZE;

    int originalA = 5000000;
    int finalA = 2500001;
    int subA = 2499999;
    uint32_t offsetA = 0;

    int originalB = 10;
    int finalB = 20000000;
    int sumB = 19999990;
    uint32_t offsetB = HOST_PAGE_SIZE + (2 * sizeof(int32_t));

    int originalC = 999999999;
    int finalC = 1;
    int subC = 999999998;
    uint32_t offsetC = offsetB + sizeof(int32_t);

    int originalD = 100000000;
    int finalD = 100000001;
    int sumD = 1;
    uint32_t offsetD = snapSize - sizeof(int32_t);

    std::shared_ptr<SnapshotData> snap =
      std::make_shared<SnapshotData>(snapPages * HOST_PAGE_SIZE);
    snap->makeRestorable(snapKey);
    uint8_t* snapData = snap->getMutableDataPtr();

    // Set up original values
    *(int*)(snapData + offsetA) = originalA;
    *(int*)(snapData + offsetB) = originalB;
    *(int*)(snapData + offsetC) = originalC;
    *(int*)(snapData + offsetD) = originalD;

    // Take the snapshot
    reg.registerSnapshot(snapKey, snap);

    // Map the snapshot to some memory
    size_t sharedMemSize = snapPages * HOST_PAGE_SIZE;
    MemoryRegion sharedMem = allocateSharedMemory(snapPages * HOST_PAGE_SIZE);

    reg.mapSnapshot(snapKey, sharedMem.get());

    // Reset dirty tracking
    faabric::util::resetDirtyTracking();

    // Set up the merge regions
    snap->addMergeRegion(offsetA,
                         sizeof(int),
                         SnapshotDataType::Int,
                         SnapshotMergeOperation::Subtract);

    snap->addMergeRegion(
      offsetB, sizeof(int), SnapshotDataType::Int, SnapshotMergeOperation::Sum);

    snap->addMergeRegion(offsetC,
                         sizeof(int),
                         SnapshotDataType::Int,
                         SnapshotMergeOperation::Subtract);

    snap->addMergeRegion(
      offsetD, sizeof(int), SnapshotDataType::Int, SnapshotMergeOperation::Sum);

    // Set final values
    *(int*)(sharedMem.get() + offsetA) = finalA;
    *(int*)(sharedMem.get() + offsetB) = finalB;
    *(int*)(sharedMem.get() + offsetC) = finalC;
    *(int*)(sharedMem.get() + offsetD) = finalD;

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
      snap->getChangeDiffs({ sharedMem.get(), sharedMemSize });
    REQUIRE(actualDiffs.size() == 4);

    checkDiffs(actualDiffs, expectedDiffs);
}

TEST_CASE_METHOD(SnapshotMergeTestFixture,
                 "Test snapshot merge regions",
                 "[snapshot][util]")
{
    std::string snapKey = "foobar123";
    int snapPages = 5;

    uint32_t offset = HOST_PAGE_SIZE + (10 * sizeof(int32_t));

    std::shared_ptr<SnapshotData> snap =
      std::make_shared<SnapshotData>(snapPages * HOST_PAGE_SIZE);
    snap->makeRestorable(snapKey);

    uint8_t* snapData = snap->getMutableDataPtr();

    std::vector<uint8_t> originalData;
    std::vector<uint8_t> updatedData;
    std::vector<uint8_t> expectedData;

    faabric::util::SnapshotDataType dataType =
      faabric::util::SnapshotDataType::Raw;
    faabric::util::SnapshotMergeOperation operation =
      faabric::util::SnapshotMergeOperation::Overwrite;

    size_t dataLength = 0;
    size_t regionLength = 0;

    SECTION("Integer")
    {
        int originalValue = 0;
        int finalValue = 0;
        int diffValue = 0;

        dataType = faabric::util::SnapshotDataType::Int;
        dataLength = sizeof(int32_t);
        regionLength = sizeof(int32_t);

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
        dataLength = 100;
        originalData = std::vector<uint8_t>(dataLength, 3);
        updatedData = std::vector<uint8_t>(dataLength, 4);
        expectedData = updatedData;

        dataType = faabric::util::SnapshotDataType::Raw;

        SECTION("Overwrite")
        {
            regionLength = dataLength;
            operation = faabric::util::SnapshotMergeOperation::Overwrite;
        }

        SECTION("Overwrite unspecified length")
        {
            regionLength = 0;
            operation = faabric::util::SnapshotMergeOperation::Overwrite;
        }
    }

    // Write the original data into place
    std::memcpy(snapData + offset, originalData.data(), originalData.size());

    // Take the snapshot
    reg.registerSnapshot(snapKey, snap);

    // Map the snapshot to some memory
    size_t sharedMemSize = snapPages * HOST_PAGE_SIZE;
    MemoryRegion sharedMem = allocateSharedMemory(snapPages * HOST_PAGE_SIZE);

    reg.mapSnapshot(snapKey, sharedMem.get());

    // Reset dirty tracking
    faabric::util::resetDirtyTracking();

    // Set up the merge region
    snap->addMergeRegion(offset, regionLength, dataType, operation);

    // Modify the value
    std::memcpy(
      sharedMem.get() + offset, updatedData.data(), updatedData.size());

    // Get the snapshot diffs
    std::vector<SnapshotDiff> actualDiffs =
      snap->getChangeDiffs({ sharedMem.get(), sharedMemSize });

    // Check diff
    REQUIRE(actualDiffs.size() == 1);
    std::vector<SnapshotDiff> expectedDiffs = { { dataType,
                                                  operation,
                                                  offset,
                                                  expectedData.data(),
                                                  expectedData.size() } };

    checkDiffs(actualDiffs, expectedDiffs);
}

TEST_CASE_METHOD(SnapshotMergeTestFixture,
                 "Test invalid snapshot merges",
                 "[snapshot][util]")
{
    std::string snapKey = "foobar123";
    int snapPages = 3;
    int offset = HOST_PAGE_SIZE + (2 * sizeof(int32_t));

    std::shared_ptr<SnapshotData> snap =
      std::make_shared<SnapshotData>(snapPages * HOST_PAGE_SIZE);
    snap->makeRestorable(snapKey);

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
    reg.registerSnapshot(snapKey, snap);

    // Map the snapshot
    size_t sharedMemSize = snapPages * HOST_PAGE_SIZE;
    MemoryRegion sharedMem = allocateSharedMemory(snapPages * HOST_PAGE_SIZE);
    reg.mapSnapshot(snapKey, sharedMem.get());

    // Reset dirty tracking
    faabric::util::resetDirtyTracking();

    // Set up the merge region
    snap->addMergeRegion(offset, dataLength, dataType, operation);

    // Modify the value
    std::vector<uint8_t> bytes(dataLength, 3);
    std::memcpy(sharedMem.get() + offset, bytes.data(), bytes.size());

    // Check getting diffs throws an exception
    bool failed = false;
    try {
        snap->getChangeDiffs({ sharedMem.get(), sharedMemSize });
    } catch (std::runtime_error& ex) {
        failed = true;
        REQUIRE(ex.what() == expectedMsg);
    }

    REQUIRE(failed);
}

TEST_CASE_METHOD(SnapshotMergeTestFixture,
                 "Test fine-grained byte-wise diffs",
                 "[snapshot][util]")
{
    int snapPages = 3;
    size_t sharedMemSize = snapPages * HOST_PAGE_SIZE;

    std::shared_ptr<SnapshotData> snap =
      std::make_shared<SnapshotData>(snapPages * HOST_PAGE_SIZE);
    snap->makeRestorable(snapKey);
    reg.registerSnapshot(snapKey, snap);

    // Map the snapshot
    MemoryRegion sharedMem = allocateSharedMemory(sharedMemSize);
    reg.mapSnapshot(snapKey, sharedMem.get());

    // Reset dirty tracking
    faabric::util::resetDirtyTracking();

    // Add some tightly-packed changes
    uint32_t offsetA = 0;
    std::vector<uint8_t> dataA(10, 1);
    std::memcpy(sharedMem.get() + offsetA, dataA.data(), dataA.size());

    uint32_t offsetB = dataA.size() + 1;
    std::vector<uint8_t> dataB(2, 1);
    std::memcpy(sharedMem.get() + offsetB, dataB.data(), dataB.size());

    uint32_t offsetC = offsetB + 3;
    std::vector<uint8_t> dataC(1, 1);
    std::memcpy(sharedMem.get() + offsetC, dataC.data(), dataC.size());

    uint32_t offsetD = offsetC + 2;
    std::vector<uint8_t> dataD(1, 1);
    std::memcpy(sharedMem.get() + offsetD, dataD.data(), dataD.size());

    std::vector<SnapshotDiff> expectedDiffs = {
        { SnapshotDataType::Raw,
          SnapshotMergeOperation::Overwrite,
          offsetA,
          dataA.data(),
          dataA.size() },
        { SnapshotDataType::Raw,
          SnapshotMergeOperation::Overwrite,
          offsetB,
          dataB.data(),
          dataB.size() },
        { SnapshotDataType::Raw,
          SnapshotMergeOperation::Overwrite,
          offsetC,
          dataC.data(),
          dataC.size() },
        { SnapshotDataType::Raw,
          SnapshotMergeOperation::Overwrite,
          offsetD,
          dataD.data(),
          dataD.size() },
    };

    // Add a single merge region for all the changes
    snap->addMergeRegion(0,
                         offsetD + dataD.size() + 20,
                         SnapshotDataType::Raw,
                         SnapshotMergeOperation::Overwrite);

    // Check number of diffs
    std::vector<SnapshotDiff> actualDiffs =
      snap->getChangeDiffs({ sharedMem.get(), sharedMemSize });

    checkDiffs(actualDiffs, expectedDiffs);
}

TEST_CASE_METHOD(SnapshotMergeTestFixture,
                 "Test mix of applicable and non-applicable merge regions",
                 "[snapshot][util]")
{
    int snapPages = 6;
    size_t sharedMemSize = snapPages * HOST_PAGE_SIZE;

    std::shared_ptr<SnapshotData> snap =
      std::make_shared<SnapshotData>(snapPages * HOST_PAGE_SIZE);
    snap->makeRestorable(snapKey);
    reg.registerSnapshot(snapKey, snap);

    // Map the snapshot
    MemoryRegion sharedMem = allocateSharedMemory(sharedMemSize);
    reg.mapSnapshot(snapKey, sharedMem.get());

    // Reset dirty tracking
    faabric::util::resetDirtyTracking();

    // Add a couple of merge regions on each page, which should be skipped as
    // they won't overlap any changes
    for (int i = 0; i < snapPages; i++) {
        // Overwrite
        int skippedOverwriteOffset = i * HOST_PAGE_SIZE;
        snap->addMergeRegion(skippedOverwriteOffset,
                             10,
                             faabric::util::SnapshotDataType::Raw,
                             faabric::util::SnapshotMergeOperation::Overwrite);

        // Sum
        int skippedSumOffset =
          ((i + 1) * HOST_PAGE_SIZE) - (2 * sizeof(int32_t));
        snap->addMergeRegion(skippedSumOffset,
                             sizeof(int32_t),
                             faabric::util::SnapshotDataType::Int,
                             faabric::util::SnapshotMergeOperation::Sum);
    }

    // Add an overwrite region that should take effect
    uint32_t overwriteAOffset = (2 * HOST_PAGE_SIZE) + 20;
    snap->addMergeRegion(overwriteAOffset,
                         20,
                         faabric::util::SnapshotDataType::Raw,
                         faabric::util::SnapshotMergeOperation::Overwrite);
    std::vector<uint8_t> overwriteData(10, 1);
    std::memcpy(sharedMem.get() + overwriteAOffset,
                overwriteData.data(),
                overwriteData.size());

    // Add a sum region and data that should also take effect
    uint32_t sumOffset = (4 * HOST_PAGE_SIZE) + 100;
    int sumValue = 333;
    int sumOriginal = 111;
    int sumExpected = 222;
    snap->addMergeRegion(sumOffset,
                         sizeof(int32_t),
                         faabric::util::SnapshotDataType::Int,
                         faabric::util::SnapshotMergeOperation::Sum);
    *(int*)(snap->getMutableDataPtr(sumOffset)) = sumOriginal;
    *(int*)(sharedMem.get() + sumOffset) = sumValue;

    // Check diffs
    std::vector<SnapshotDiff> expectedDiffs = {
        { faabric::util::SnapshotDataType::Raw,
          faabric::util::SnapshotMergeOperation::Overwrite,
          overwriteAOffset,
          BYTES(overwriteData.data()),
          overwriteData.size() },
        { faabric::util::SnapshotDataType::Int,
          faabric::util::SnapshotMergeOperation::Sum,
          sumOffset,
          BYTES(&sumExpected),
          sizeof(int32_t) },
    };

    std::vector<SnapshotDiff> actualDiffs =
      snap->getChangeDiffs({ sharedMem.get(), sharedMemSize });

    checkDiffs(actualDiffs, expectedDiffs);
}

TEST_CASE_METHOD(SnapshotMergeTestFixture,
                 "Test overwrite region to end of memory",
                 "[snapshot][util]")
{
    int snapPages = 6;
    int sharedMemPages = 10;
    size_t sharedMemSize = sharedMemPages * HOST_PAGE_SIZE;

    std::shared_ptr<SnapshotData> snap =
      std::make_shared<SnapshotData>(snapPages * HOST_PAGE_SIZE);
    snap->makeRestorable(snapKey);
    reg.registerSnapshot(snapKey, snap);

    // Map the snapshot
    MemoryRegion sharedMem = allocateSharedMemory(sharedMemSize);
    reg.mapSnapshot(snapKey, sharedMem.get());
    faabric::util::resetDirtyTracking();

    // Make an edit somewhere in the extended memory, outside the original
    // snapshot
    uint32_t diffPageStart = (snapPages + 2) * HOST_PAGE_SIZE;
    uint32_t diffOffset = diffPageStart + 100;
    std::vector<uint8_t> diffData(120, 2);
    std::memcpy(sharedMem.get() + diffOffset, diffData.data(), diffData.size());

    // Add a merge region from near end of original snapshot upwards
    snap->addMergeRegion(snap->size - 120,
                         0,
                         faabric::util::SnapshotDataType::Raw,
                         faabric::util::SnapshotMergeOperation::Overwrite);

    std::vector<SnapshotDiff> actualDiffs =
      snap->getChangeDiffs({ sharedMem.get(), sharedMemSize });

    // Make sure the whole page containing the diff is included
    std::vector<SnapshotDiff> expectedDiffs = {
        { faabric::util::SnapshotDataType::Raw,
          faabric::util::SnapshotMergeOperation::Overwrite,
          diffPageStart,
          sharedMem.get() + diffPageStart,
          (size_t)HOST_PAGE_SIZE },
    };

    checkDiffs(actualDiffs, expectedDiffs);
}

TEST_CASE("Test snapshot data constructors", "[snapshot][util]")
{
    std::vector<uint8_t> data(2 * HOST_PAGE_SIZE, 3);

    // Add known subsection
    uint32_t chunkOffset = 120;
    std::vector<uint8_t> chunk(100, 4);
    ::memcpy(data.data() + chunkOffset, chunk.data(), chunk.size());

    bool expectOwner = false;
    size_t expectedMaxSize = data.size();

    std::shared_ptr<SnapshotData> snap = nullptr;
    SECTION("From size")
    {
        SECTION("No max")
        {
            snap = std::make_shared<SnapshotData>(data.size());
        }

        SECTION("Zero max")
        {
            snap = std::make_shared<SnapshotData>(data.size(), 0);
        }

        SECTION("With max")
        {
            expectedMaxSize = data.size() + 123;
            snap = std::make_shared<SnapshotData>(data.size(), expectedMaxSize);
        }

        snap->copyInData(data, 0);
        expectOwner = true;
    }

    SECTION("From span")
    {
        snap = std::make_shared<SnapshotData>(
          std::span<uint8_t>(data.data(), data.size()));
    }

    SECTION("From vector") { snap = std::make_shared<SnapshotData>(data); }

    REQUIRE(snap->size == data.size());
    REQUIRE(snap->maxSize == expectedMaxSize);
    REQUIRE(snap->isOwner() == expectOwner);

    std::vector<uint8_t> actualCopy = snap->getDataCopy();
    REQUIRE(actualCopy == data);

    std::vector<uint8_t> actualChunk =
      snap->getDataCopy(chunkOffset, chunk.size());
    REQUIRE(actualChunk == chunk);

    const std::vector<uint8_t> actualConst(snap->getDataPtr(),
                                           snap->getDataPtr() + snap->size);
    REQUIRE(actualConst == data);

    std::vector<uint8_t> actualMutable(snap->getMutableDataPtr(),
                                       snap->getMutableDataPtr() + snap->size);
    REQUIRE(actualMutable == data);
}

}
