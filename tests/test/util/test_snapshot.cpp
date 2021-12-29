#include <catch2/catch.hpp>

#include "faabric_utils.h"
#include "fixtures.h"

#include <faabric/snapshot/SnapshotRegistry.h>
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

        reg.registerSnapshot(snapKey, snapData);

        return snapData;
    }

    void checkDiffs(std::vector<SnapshotDiff>& actualDiffs,
                    std::vector<SnapshotDiff>& expectedDiffs)
    {
        REQUIRE(actualDiffs.size() == expectedDiffs.size());

        for (int i = 0; i < actualDiffs.size(); i++) {
            SnapshotDiff& actualDiff = actualDiffs.at(i);
            SnapshotDiff& expectedDiff = expectedDiffs.at(i);

            REQUIRE(actualDiff.getOperation() == expectedDiff.getOperation());
            REQUIRE(actualDiff.getDataType() == expectedDiff.getDataType());
            REQUIRE(actualDiff.getOffset() == expectedDiff.getOffset());

            std::vector<uint8_t> actualData = actualDiff.getDataCopy();
            std::vector<uint8_t> expectedData = expectedDiff.getDataCopy();
            REQUIRE(actualData == expectedData);
        }
    }
};

TEST_CASE_METHOD(SnapshotMergeTestFixture,
                 "Test snapshot diff operations",
                 "[snapshot][util]")
{
    std::vector<uint8_t> dataA(100, 1);

    std::vector<uint8_t> dataB(2 * HOST_PAGE_SIZE, 2);
    MemoryRegion memB = allocatePrivateMemory(dataB.size());
    std::memcpy(memB.get(), dataB.data(), dataB.size());

    std::vector<uint8_t> dataC(sizeof(int), 3);

    uint32_t offsetA = 10;
    uint32_t offsetB = 5 * HOST_PAGE_SIZE;
    uint32_t offsetC = 10 * HOST_PAGE_SIZE;

    SnapshotDiff diffA(
      SnapshotDataType::Raw, SnapshotMergeOperation::Overwrite, offsetA, dataA);

    SnapshotDiff diffB(SnapshotDataType::Raw,
                       SnapshotMergeOperation::Overwrite,
                       offsetB,
                       std::span<const uint8_t>(memB.get(), dataB.size()));

    SnapshotDiff diffC(
      SnapshotDataType::Int, SnapshotMergeOperation::Sum, offsetC, dataC);

    REQUIRE(diffA.getOffset() == offsetA);
    REQUIRE(diffB.getOffset() == offsetB);
    REQUIRE(diffC.getOffset() == offsetC);

    REQUIRE(diffA.getDataType() == SnapshotDataType::Raw);
    REQUIRE(diffB.getDataType() == SnapshotDataType::Raw);
    REQUIRE(diffC.getDataType() == SnapshotDataType::Int);

    REQUIRE(diffA.getOperation() == SnapshotMergeOperation::Overwrite);
    REQUIRE(diffB.getOperation() == SnapshotMergeOperation::Overwrite);
    REQUIRE(diffC.getOperation() == SnapshotMergeOperation::Sum);

    REQUIRE(diffA.getDataCopy() == dataA);
    REQUIRE(diffB.getDataCopy() == dataB);
    REQUIRE(diffC.getDataCopy() == dataC);
}

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

    snap->copyInData(dataA, 2 * HOST_PAGE_SIZE);
    snap->copyInData(dataB, 5 * HOST_PAGE_SIZE + 2);

    // Record the snap memory
    std::vector<uint8_t> actualSnapMem = snap->getDataCopy();

    // Set up shared memory
    int sharedMemPages = 20;
    size_t sharedMemSize = sharedMemPages * HOST_PAGE_SIZE;
    MemoryRegion sharedMem = allocatePrivateMemory(sharedMemSize);

    // Check it's zeroed
    std::vector<uint8_t> expectedInitial(snap->getSize(), 0);
    std::vector<uint8_t> actualSharedMemBefore(
      sharedMem.get(), sharedMem.get() + snap->getSize());
    REQUIRE(actualSharedMemBefore == expectedInitial);

    // Map the snapshot and check again
    snap->mapToMemory({ sharedMem.get(), snap->getSize() });
    std::vector<uint8_t> actualSharedMemAfter(
      sharedMem.get(), sharedMem.get() + snap->getSize());
    REQUIRE(actualSharedMemAfter == actualSnapMem);
}

TEST_CASE_METHOD(SnapshotMergeTestFixture,
                 "Test mapping editing and remapping memory",
                 "[snapshot][util]")
{
    int snapPages = 4;
    size_t snapSize = snapPages * HOST_PAGE_SIZE;
    auto snap = std::make_shared<SnapshotData>(snapSize);

    std::vector<uint8_t> initialData(100, 2);
    std::vector<uint8_t> dataA(150, 3);
    std::vector<uint8_t> dataB(200, 4);

    // Deliberately use offsets on the same page to check copy-on-write mappings
    uint32_t initialOffset = 0;
    uint32_t offsetA = HOST_PAGE_SIZE;
    uint32_t offsetB = HOST_PAGE_SIZE + dataA.size() + 1;

    // Set up some initial data
    snap->copyInData(initialData, initialOffset);
    std::vector<uint8_t> expectedSnapMem = snap->getDataCopy();

    // Set up two shared mem regions
    MemoryRegion sharedMemA = allocatePrivateMemory(snapSize);
    MemoryRegion sharedMemB = allocatePrivateMemory(snapSize);

    // Map the snapshot and both regions reflect the change
    snap->mapToMemory({ sharedMemA.get(), snapSize });
    snap->mapToMemory({ sharedMemB.get(), snapSize });

    REQUIRE(std::vector(sharedMemA.get(), sharedMemA.get() + snapSize) ==
            expectedSnapMem);
    REQUIRE(std::vector(sharedMemB.get(), sharedMemB.get() + snapSize) ==
            expectedSnapMem);

    // Reset dirty tracking
    faabric::util::resetDirtyTracking();

    // Make different edits to both mapped regions, check they are not
    // propagated back to the snapshot
    std::memcpy(sharedMemA.get() + offsetA, dataA.data(), dataA.size());
    std::memcpy(sharedMemB.get() + offsetB, dataB.data(), dataB.size());

    std::vector<uint8_t> actualSnapMem = snap->getDataCopy();
    REQUIRE(actualSnapMem == expectedSnapMem);

    // Set two separate merge regions to cover both changes
    snap->addMergeRegion(offsetA,
                         dataA.size(),
                         SnapshotDataType::Raw,
                         SnapshotMergeOperation::Overwrite);

    snap->addMergeRegion(offsetB,
                         dataB.size(),
                         SnapshotDataType::Raw,
                         SnapshotMergeOperation::Overwrite);

    // Apply diffs from both snapshots
    std::vector<SnapshotDiff> diffsA =
      MemoryView({ sharedMemA.get(), snapSize }).diffWithSnapshot(snap);
    std::vector<SnapshotDiff> diffsB =
      MemoryView({ sharedMemB.get(), snapSize }).diffWithSnapshot(snap);

    REQUIRE(diffsA.size() == 1);
    SnapshotDiff& diffA = diffsA.front();
    REQUIRE(diffA.getData().size() == dataA.size());
    REQUIRE(diffA.getOffset() == offsetA);

    REQUIRE(diffsB.size() == 1);
    SnapshotDiff& diffB = diffsB.front();
    REQUIRE(diffB.getData().size() == dataB.size());
    REQUIRE(diffB.getOffset() == offsetB);

    snap->queueDiffs(diffsA);
    REQUIRE(snap->getQueuedDiffsCount() == diffsA.size());

    snap->queueDiffs(diffsB);
    REQUIRE(snap->getQueuedDiffsCount() == diffsA.size() + diffsB.size());

    snap->writeQueuedDiffs();

    // Make sure snapshot now includes both
    std::memcpy(expectedSnapMem.data() + offsetA, dataA.data(), dataA.size());
    std::memcpy(expectedSnapMem.data() + offsetB, dataB.data(), dataB.size());

    actualSnapMem = snap->getDataCopy();
    REQUIRE(actualSnapMem == expectedSnapMem);
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

    // Map to some other region of memory large enough for the extended version
    MemoryRegion sharedMem = allocatePrivateMemory(expandedSize);
    snap->mapToMemory({ sharedMem.get(), originalSize });

    // Add some data to the extended region. Check the snapshot extends to fit
    std::vector<uint8_t> dataC(300, 5);
    std::vector<uint8_t> dataD(200, 6);
    uint32_t extendedOffsetA = (originalPages + 3) * HOST_PAGE_SIZE;
    uint32_t extendedOffsetB = (originalPages + 5) * HOST_PAGE_SIZE;

    snap->copyInData(dataC, extendedOffsetA);
    size_t expectedSizeA = extendedOffsetA + dataC.size();
    REQUIRE(snap->getSize() == expectedSizeA);

    snap->copyInData(dataD, extendedOffsetB);
    size_t expectedSizeB = extendedOffsetB + dataD.size();
    REQUIRE(snap->getSize() == expectedSizeB);

    // Remap to shared memory
    snap->mapToMemory({ sharedMem.get(), snap->getSize() });

    // Check mapped region matches
    std::vector<uint8_t> actualData = snap->getDataCopy();
    std::vector<uint8_t> actualSharedMem(sharedMem.get(),
                                         sharedMem.get() + snap->getSize());

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

    // Set up some integers in the snapshot
    int intAOffset = HOST_PAGE_SIZE + (10 * sizeof(int32_t));
    int intBOffset = (2 * HOST_PAGE_SIZE) + (20 * sizeof(int32_t));

    // Set the original values
    snap->copyInData({ BYTES(&originalValueA), sizeof(int) }, intAOffset);
    snap->copyInData({ BYTES(&originalValueB), sizeof(int) }, intBOffset);

    // Take the snapshot
    reg.registerSnapshot(snapKey, snap);

    // Map the snapshot to some memory
    MemoryRegion sharedMem = allocatePrivateMemory(memSize);
    snap->mapToMemory({ sharedMem.get(), memSize });

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
      MemoryView({ sharedMem.get(), memSize }).diffWithSnapshot(snap);

    // Check original hasn't changed
    const uint8_t* rawSnapData = snap->getDataPtr();
    int actualA = *(int*)(rawSnapData + intAOffset);
    int actualB = *(int*)(rawSnapData + intBOffset);
    REQUIRE(actualA == originalValueA);
    REQUIRE(actualB == originalValueB);

    // Check diffs themselves
    REQUIRE(actualDiffs.size() == 2);

    SnapshotDiff& diffA = actualDiffs.at(0);
    SnapshotDiff& diffB = actualDiffs.at(1);

    REQUIRE(diffA.getOffset() == intAOffset);
    REQUIRE(diffB.getOffset() == intBOffset);

    REQUIRE(diffA.getOperation() == SnapshotMergeOperation::Sum);
    REQUIRE(diffB.getOperation() == SnapshotMergeOperation::Sum);

    REQUIRE(diffA.getDataType() == SnapshotDataType::Int);
    REQUIRE(diffB.getDataType() == SnapshotDataType::Int);

    REQUIRE(diffA.getData().size() == sizeof(int32_t));
    REQUIRE(diffB.getData().size() == sizeof(int32_t));

    // Check that original values have been subtracted from final values for
    // sums
    REQUIRE(*(int*)diffA.getData().data() == sumValueA);
    REQUIRE(*(int*)diffB.getData().data() == sumValueB);
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

    // Set up original values
    snap->copyInData({ BYTES(&originalA), sizeof(int) }, offsetA);
    snap->copyInData({ BYTES(&originalB), sizeof(int) }, offsetB);
    snap->copyInData({ BYTES(&originalC), sizeof(int) }, offsetC);
    snap->copyInData({ BYTES(&originalD), sizeof(int) }, offsetD);

    // Take the snapshot
    reg.registerSnapshot(snapKey, snap);

    // Map the snapshot to some memory
    size_t sharedMemSize = snapPages * HOST_PAGE_SIZE;
    MemoryRegion sharedMem = allocatePrivateMemory(sharedMemSize);

    snap->mapToMemory({ sharedMem.get(), sharedMemSize });

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
          { BYTES(&subA), sizeof(int32_t) } },
        { SnapshotDataType::Int,
          SnapshotMergeOperation::Sum,
          offsetB,
          { BYTES(&sumB), sizeof(int32_t) } },
        { SnapshotDataType::Int,
          SnapshotMergeOperation::Subtract,
          offsetC,
          { BYTES(&subC), sizeof(int32_t) } },
        { SnapshotDataType::Int,
          SnapshotMergeOperation::Sum,
          offsetD,
          { BYTES(&sumD), sizeof(int32_t) } },
    };

    std::vector<SnapshotDiff> actualDiffs =
      MemoryView({ sharedMem.get(), sharedMemSize }).diffWithSnapshot(snap);
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

    std::vector<uint8_t> originalData;
    std::vector<uint8_t> updatedData;
    std::vector<uint8_t> expectedData;

    faabric::util::SnapshotDataType dataType =
      faabric::util::SnapshotDataType::Raw;
    faabric::util::SnapshotMergeOperation operation =
      faabric::util::SnapshotMergeOperation::Overwrite;

    size_t dataLength = 0;
    size_t regionLength = 0;

    bool expectNoDiff = false;

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

    SECTION("Long")
    {
        long originalValue = 0;
        long finalValue = 0;
        long diffValue = 0;

        dataType = faabric::util::SnapshotDataType::Long;
        dataLength = sizeof(long);
        regionLength = sizeof(long);

        SECTION("Long sum")
        {
            originalValue = 100;
            finalValue = 150;
            diffValue = 50;

            operation = faabric::util::SnapshotMergeOperation::Sum;
        }

        SECTION("Long subtract")
        {
            originalValue = 150;
            finalValue = 100;
            diffValue = 50;

            operation = faabric::util::SnapshotMergeOperation::Subtract;
        }

        SECTION("Long product")
        {
            originalValue = 3;
            finalValue = 150;
            diffValue = 50;

            operation = faabric::util::SnapshotMergeOperation::Product;
        }

        SECTION("Long max")
        {
            originalValue = 10;
            finalValue = 200;
            diffValue = 200;

            operation = faabric::util::SnapshotMergeOperation::Max;
        }

        SECTION("Long min")
        {
            originalValue = 30;
            finalValue = 10;
            diffValue = 10;

            operation = faabric::util::SnapshotMergeOperation::Max;
        }

        originalData = faabric::util::valueToBytes<long>(originalValue);
        updatedData = faabric::util::valueToBytes<long>(finalValue);
        expectedData = faabric::util::valueToBytes<long>(diffValue);
    }

    SECTION("Float")
    {
        float originalValue = 0.0;
        float finalValue = 0.0;
        float diffValue = 0.0;

        dataType = faabric::util::SnapshotDataType::Float;
        dataLength = sizeof(float);
        regionLength = sizeof(float);

        // Note - imprecision in float arithmetic makes it difficult to test
        // the floating point types here unless we use integer values.
        SECTION("Float sum")
        {
            originalValue = 513;
            finalValue = 945;
            diffValue = 432;

            operation = faabric::util::SnapshotMergeOperation::Sum;
        }

        SECTION("Float subtract")
        {
            originalValue = 945;
            finalValue = 513;
            diffValue = 432;

            operation = faabric::util::SnapshotMergeOperation::Subtract;
        }

        SECTION("Float product")
        {
            originalValue = 5;
            finalValue = 655;
            diffValue = 131;

            operation = faabric::util::SnapshotMergeOperation::Product;
        }

        SECTION("Float max")
        {
            originalValue = 555.55;
            finalValue = 666.66;
            diffValue = 666.66;

            operation = faabric::util::SnapshotMergeOperation::Max;
        }

        SECTION("Float min")
        {
            originalValue = 222.22;
            finalValue = 333.33;
            diffValue = 333.33;

            operation = faabric::util::SnapshotMergeOperation::Min;
        }

        originalData = faabric::util::valueToBytes<float>(originalValue);
        updatedData = faabric::util::valueToBytes<float>(finalValue);
        expectedData = faabric::util::valueToBytes<float>(diffValue);
    }

    SECTION("Double")
    {
        double originalValue = 0.0;
        double finalValue = 0.0;
        double diffValue = 0.0;

        dataType = faabric::util::SnapshotDataType::Double;
        dataLength = sizeof(double);
        regionLength = sizeof(double);

        // Note - imprecision in float arithmetic makes it difficult to test
        // the floating point types here unless we use integer values.
        SECTION("Double sum")
        {
            originalValue = 513;
            finalValue = 945;
            diffValue = 432;

            operation = faabric::util::SnapshotMergeOperation::Sum;
        }

        SECTION("Double subtract")
        {
            originalValue = 945;
            finalValue = 513;
            diffValue = 432;

            operation = faabric::util::SnapshotMergeOperation::Subtract;
        }

        SECTION("Double product")
        {
            originalValue = 5;
            finalValue = 655;
            diffValue = 131;

            operation = faabric::util::SnapshotMergeOperation::Product;
        }

        SECTION("Double max")
        {
            originalValue = 555.55;
            finalValue = 666.66;
            diffValue = 666.66;

            operation = faabric::util::SnapshotMergeOperation::Max;
        }

        SECTION("Double min")
        {
            originalValue = 222.22;
            finalValue = 333.33;
            diffValue = 333.33;

            operation = faabric::util::SnapshotMergeOperation::Min;
        }

        originalData = faabric::util::valueToBytes<double>(originalValue);
        updatedData = faabric::util::valueToBytes<double>(finalValue);
        expectedData = faabric::util::valueToBytes<double>(diffValue);
    }

    SECTION("Bool")
    {
        bool originalValue = false;
        bool finalValue = false;
        bool diffValue = false;

        dataType = faabric::util::SnapshotDataType::Bool;
        dataLength = sizeof(bool);
        regionLength = sizeof(bool);

        SECTION("Bool overwrite")
        {
            originalValue = true;
            finalValue = false;
            diffValue = false;

            operation = faabric::util::SnapshotMergeOperation::Overwrite;
        }

        originalData = faabric::util::valueToBytes<bool>(originalValue);
        updatedData = faabric::util::valueToBytes<bool>(finalValue);
        expectedData = faabric::util::valueToBytes<bool>(diffValue);
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

        SECTION("Ignore")
        {
            regionLength = dataLength;
            operation = faabric::util::SnapshotMergeOperation::Ignore;

            expectedData = originalData;
            expectNoDiff = true;
        }
    }

    // Write the original data into place
    snap->copyInData(originalData, offset);

    // Register the snap
    reg.registerSnapshot(snapKey, snap);

    // Map the snapshot to some memory
    size_t sharedMemSize = snapPages * HOST_PAGE_SIZE;
    MemoryRegion sharedMem = allocatePrivateMemory(sharedMemSize);

    snap->mapToMemory({ sharedMem.get(), sharedMemSize });

    // Reset dirty tracking
    faabric::util::resetDirtyTracking();

    // Set up the merge region
    snap->addMergeRegion(offset, regionLength, dataType, operation);

    // Modify the value
    std::memcpy(
      sharedMem.get() + offset, updatedData.data(), updatedData.size());

    // Get the snapshot diffs
    std::vector<SnapshotDiff> actualDiffs =
      MemoryView({ sharedMem.get(), sharedMemSize }).diffWithSnapshot(snap);

    if (expectNoDiff) {
        REQUIRE(actualDiffs.empty());
    } else {
        // Check diff
        REQUIRE(actualDiffs.size() == 1);
        std::vector<SnapshotDiff> expectedDiffs = {
            { dataType,
              operation,
              offset,
              { expectedData.data(), expectedData.size() } }
        };

        checkDiffs(actualDiffs, expectedDiffs);
    }
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

    faabric::util::SnapshotDataType dataType =
      faabric::util::SnapshotDataType::Raw;
    faabric::util::SnapshotMergeOperation operation =
      faabric::util::SnapshotMergeOperation::Overwrite;
    size_t dataLength = 0;

    std::string expectedMsg;

    SECTION("Bool product")
    {
        dataType = faabric::util::SnapshotDataType::Bool;
        operation = faabric::util::SnapshotMergeOperation::Product;
        dataLength = sizeof(bool);
        expectedMsg = "Unsupported merge op combination";
    }

    SECTION("Raw sum")
    {
        dataType = faabric::util::SnapshotDataType::Raw;
        operation = faabric::util::SnapshotMergeOperation::Sum;
        dataLength = 123;
        expectedMsg = "Unsupported merge op combination";
    }

    // Take the snapshot
    reg.registerSnapshot(snapKey, snap);

    // Map the snapshot
    size_t sharedMemSize = snapPages * HOST_PAGE_SIZE;
    MemoryRegion sharedMem = allocatePrivateMemory(sharedMemSize);
    snap->mapToMemory({ sharedMem.get(), sharedMemSize });

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
        MemoryView({ sharedMem.get(), sharedMemSize }).diffWithSnapshot(snap);
    } catch (std::runtime_error& ex) {
        failed = true;
        REQUIRE(ex.what() == expectedMsg);
    }

    REQUIRE(failed);
}

TEST_CASE_METHOD(SnapshotMergeTestFixture,
                 "Test diffing snapshot memory",
                 "[snapshot][util]")
{
    // Set up a snapshot
    int snapPages = 4;
    size_t snapSize = snapPages * HOST_PAGE_SIZE;

    std::shared_ptr<SnapshotData> snap =
      std::make_shared<SnapshotData>(snapSize);
    reg.registerSnapshot(snapKey, snap);

    // Check memory is zeroed initially
    std::vector<uint8_t> zeroes(snapSize, 0);
    REQUIRE(snap->getDataCopy() == zeroes);

    // Check memory is page-aligned
    faabric::util::isPageAligned((const void*)snap->getDataPtr());

    // Reset dirty tracking
    faabric::util::resetDirtyTracking();

    // Update the snapshot
    std::vector<uint8_t> dataA = { 0, 1, 2, 3 };
    std::vector<uint8_t> dataB = { 3, 4, 5 };
    uint32_t offsetA = 0;
    uint32_t offsetB = 2 * HOST_PAGE_SIZE + 1;

    snap->copyInData(dataA, offsetA);
    snap->copyInData(dataB, offsetB);

    // Check we get the expected diffs
    std::vector<SnapshotDiff> expectedDiffs =
      MemoryView({ snap->getDataPtr(), snap->getSize() }).getDirtyRegions();

    REQUIRE(expectedDiffs.size() == 2);

    SnapshotDiff& diffA = expectedDiffs.at(0);
    SnapshotDiff& diffB = expectedDiffs.at(1);

    REQUIRE(diffA.getData().size() == HOST_PAGE_SIZE);
    REQUIRE(diffB.getData().size() == HOST_PAGE_SIZE);

    std::vector<uint8_t> actualA = { diffA.getData().begin(),
                                     diffA.getData().begin() + dataA.size() };
    std::vector<uint8_t> actualB = {
        diffB.getData().begin() + 1, diffB.getData().begin() + 1 + dataB.size()
    };

    REQUIRE(actualA == dataA);
    REQUIRE(actualB == dataB);
}

TEST_CASE_METHOD(SnapshotMergeTestFixture,
                 "Test fine-grained byte-wise diffs",
                 "[snapshot][util]")
{
    int snapPages = 3;
    size_t sharedMemSize = snapPages * HOST_PAGE_SIZE;

    std::shared_ptr<SnapshotData> snap =
      std::make_shared<SnapshotData>(snapPages * HOST_PAGE_SIZE);
    reg.registerSnapshot(snapKey, snap);

    // Map the snapshot
    MemoryRegion sharedMem = allocatePrivateMemory(sharedMemSize);
    snap->mapToMemory({ sharedMem.get(), sharedMemSize });

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
          { dataA.data(), dataA.size() } },
        { SnapshotDataType::Raw,
          SnapshotMergeOperation::Overwrite,
          offsetB,
          { dataB.data(), dataB.size() } },
        { SnapshotDataType::Raw,
          SnapshotMergeOperation::Overwrite,
          offsetC,
          { dataC.data(), dataC.size() } },
        { SnapshotDataType::Raw,
          SnapshotMergeOperation::Overwrite,
          offsetD,
          { dataD.data(), dataD.size() } },
    };

    // Add a single merge region for all the changes
    snap->addMergeRegion(0,
                         offsetD + dataD.size() + 20,
                         SnapshotDataType::Raw,
                         SnapshotMergeOperation::Overwrite);

    // Check number of diffs
    std::vector<SnapshotDiff> actualDiffs =
      MemoryView({ sharedMem.get(), sharedMemSize }).diffWithSnapshot(snap);

    checkDiffs(actualDiffs, expectedDiffs);
}

TEST_CASE_METHOD(SnapshotMergeTestFixture,
                 "Test filling gaps in regions with overwrite",
                 "[snapshot][util]")
{
    int snapPages = 3;
    size_t snapSize = snapPages * HOST_PAGE_SIZE;

    std::shared_ptr<SnapshotData> snap =
      std::make_shared<SnapshotData>(snapSize);

    std::map<uint32_t, SnapshotMergeRegion> expectedRegions;

    SECTION("No existing regions")
    {
        expectedRegions[0] = {
            0, 0, SnapshotDataType::Raw, SnapshotMergeOperation::Overwrite
        };
    }

    SECTION("One region at start")
    {
        snap->addMergeRegion(
          0, 100, SnapshotDataType::Raw, SnapshotMergeOperation::Overwrite);
        expectedRegions[0] = {
            0, 100, SnapshotDataType::Raw, SnapshotMergeOperation::Overwrite
        };
        expectedRegions[100] = {
            100, 0, SnapshotDataType::Raw, SnapshotMergeOperation::Overwrite
        };
    }

    SECTION("One region at end")
    {
        snap->addMergeRegion(snapSize - 100,
                             100,
                             SnapshotDataType::Raw,
                             SnapshotMergeOperation::Overwrite);

        expectedRegions[0] = { 0,
                               snapSize - 100,
                               SnapshotDataType::Raw,
                               SnapshotMergeOperation::Overwrite };

        expectedRegions[snapSize - 100] = { (uint32_t)snapSize - 100,
                                            100,
                                            SnapshotDataType::Raw,
                                            SnapshotMergeOperation::Overwrite };
    }

    SECTION("Multiple regions")
    {
        // Deliberately add out of order
        snap->addMergeRegion(HOST_PAGE_SIZE,
                             sizeof(double),
                             SnapshotDataType::Double,
                             SnapshotMergeOperation::Product);

        snap->addMergeRegion(
          100, sizeof(int), SnapshotDataType::Int, SnapshotMergeOperation::Sum);

        snap->addMergeRegion(HOST_PAGE_SIZE + 200,
                             HOST_PAGE_SIZE,
                             SnapshotDataType::Raw,
                             SnapshotMergeOperation::Overwrite);

        expectedRegions[0] = {
            0, 100, SnapshotDataType::Raw, SnapshotMergeOperation::Overwrite
        };

        expectedRegions[100] = {
            100, sizeof(int), SnapshotDataType::Int, SnapshotMergeOperation::Sum
        };

        expectedRegions[100 + sizeof(int)] = {
            100 + sizeof(int),
            HOST_PAGE_SIZE - (100 + sizeof(int)),
            SnapshotDataType::Raw,
            SnapshotMergeOperation::Overwrite
        };

        expectedRegions[HOST_PAGE_SIZE] = { (uint32_t)HOST_PAGE_SIZE,
                                            sizeof(double),
                                            SnapshotDataType::Double,
                                            SnapshotMergeOperation::Product };

        expectedRegions[HOST_PAGE_SIZE + sizeof(double)] = {
            (uint32_t)(HOST_PAGE_SIZE + sizeof(double)),
            200 - sizeof(double),
            SnapshotDataType::Raw,
            SnapshotMergeOperation::Overwrite
        };

        expectedRegions[HOST_PAGE_SIZE + 200] = {
            (uint32_t)HOST_PAGE_SIZE + 200,
            (uint32_t)HOST_PAGE_SIZE,
            SnapshotDataType::Raw,
            SnapshotMergeOperation::Overwrite
        };

        expectedRegions[(2 * HOST_PAGE_SIZE) + 200] = {
            (uint32_t)(2 * HOST_PAGE_SIZE) + 200,
            0,
            SnapshotDataType::Raw,
            SnapshotMergeOperation::Overwrite
        };
    }

    snap->fillGapsWithOverwriteRegions();

    std::map<uint32_t, SnapshotMergeRegion> actualRegions =
      snap->getMergeRegions();

    REQUIRE(actualRegions.size() == expectedRegions.size());
    for (auto [expectedOffset, expectedRegion] : expectedRegions) {
        REQUIRE(actualRegions.find(expectedOffset) != actualRegions.end());

        SnapshotMergeRegion actualRegion = actualRegions[expectedOffset];
        REQUIRE(actualRegion.offset == expectedRegion.offset);
        REQUIRE(actualRegion.dataType == expectedRegion.dataType);
        REQUIRE(actualRegion.length == expectedRegion.length);
        REQUIRE(actualRegion.operation == expectedRegion.operation);
    }
}

TEST_CASE_METHOD(SnapshotMergeTestFixture,
                 "Test mix of applicable and non-applicable merge regions",
                 "[snapshot][util]")
{
    int snapPages = 6;
    size_t sharedMemSize = snapPages * HOST_PAGE_SIZE;

    std::shared_ptr<SnapshotData> snap =
      std::make_shared<SnapshotData>(snapPages * HOST_PAGE_SIZE);
    reg.registerSnapshot(snapKey, snap);

    // Map the snapshot
    MemoryRegion sharedMem = allocatePrivateMemory(sharedMemSize);
    snap->mapToMemory({ sharedMem.get(), sharedMemSize });

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
    snap->copyInData({ BYTES(&sumOriginal), sizeof(int) }, sumOffset);
    *(int*)(sharedMem.get() + sumOffset) = sumValue;

    // Check diffs
    std::vector<SnapshotDiff> expectedDiffs = {
        { faabric::util::SnapshotDataType::Raw,
          faabric::util::SnapshotMergeOperation::Overwrite,
          overwriteAOffset,
          { BYTES(overwriteData.data()), overwriteData.size() } },
        { faabric::util::SnapshotDataType::Int,
          faabric::util::SnapshotMergeOperation::Sum,
          sumOffset,
          { BYTES(&sumExpected), sizeof(int32_t) } },
    };

    std::vector<SnapshotDiff> actualDiffs =
      MemoryView({ sharedMem.get(), sharedMemSize }).diffWithSnapshot(snap);

    checkDiffs(actualDiffs, expectedDiffs);
}

TEST_CASE_METHOD(SnapshotMergeTestFixture,
                 "Test merge regions past end of original memory",
                 "[snapshot][util]")
{
    int snapPages = 6;
    int sharedMemPages = 10;
    size_t snapSize = snapPages * HOST_PAGE_SIZE;
    size_t sharedMemSize = sharedMemPages * HOST_PAGE_SIZE;

    std::shared_ptr<SnapshotData> snap =
      std::make_shared<SnapshotData>(snapSize);
    reg.registerSnapshot(snapKey, snap);

    // Map the snapshot
    MemoryRegion sharedMem = allocatePrivateMemory(sharedMemSize);
    snap->mapToMemory({ sharedMem.get(), snapSize });
    faabric::util::resetDirtyTracking();

    uint32_t changeStartPage = 0;
    uint32_t changeOffset = 0;
    uint32_t mergeRegionStart = snapSize;
    size_t changeLength = 123;

    uint32_t expectedDiffStart = 0;
    uint32_t expectedDiffSize = 0;

    // When memory has changed at or past the end of the original data, the diff
    // will start at the end of the original data and round up to the next page
    // boundary. If the change starts before the end, it will start at the
    // beginning of the change and continue into the page boundary past the
    // original data.

    SECTION("Change at end of original data, overlapping merge region")
    {
        changeStartPage = snapSize;
        changeOffset = changeStartPage + 100;
        mergeRegionStart = snapSize;
        expectedDiffStart = changeStartPage;
        expectedDiffSize = HOST_PAGE_SIZE;
    }

    SECTION("Change and merge region aligned at end of original data")
    {
        changeStartPage = snapSize;
        changeOffset = changeStartPage;
        mergeRegionStart = snapSize;
        expectedDiffStart = changeStartPage;
        expectedDiffSize = HOST_PAGE_SIZE;
    }

    SECTION("Change after end of original data, overlapping merge region")
    {
        changeStartPage = (snapPages + 2) * HOST_PAGE_SIZE;
        changeOffset = changeStartPage + 100;
        mergeRegionStart = changeStartPage;
        expectedDiffStart = changeStartPage;
        expectedDiffSize = HOST_PAGE_SIZE;
    }

    SECTION("Merge region and change crossing end of original data")
    {
        // Merge region starts before diff
        changeStartPage = (snapPages - 1) * HOST_PAGE_SIZE;
        changeOffset = changeStartPage + 100;
        mergeRegionStart = (snapPages - 2) * HOST_PAGE_SIZE;

        // Change goes from inside original data to overshoot the end
        changeLength = 2 * HOST_PAGE_SIZE;

        // Diff will cover from the start of the change to round up to the
        // nearest page in the overshoot region.
        expectedDiffStart = changeOffset;
        expectedDiffSize = changeLength + (HOST_PAGE_SIZE - 100);
    }

    std::vector<uint8_t> diffData(changeLength, 2);
    std::memcpy(
      sharedMem.get() + changeOffset, diffData.data(), diffData.size());

    // Add a merge region
    snap->addMergeRegion(mergeRegionStart,
                         0,
                         faabric::util::SnapshotDataType::Raw,
                         faabric::util::SnapshotMergeOperation::Overwrite);

    std::vector<SnapshotDiff> actualDiffs =
      MemoryView({ sharedMem.get(), sharedMemSize }).diffWithSnapshot(snap);

    // Set up expected diff
    std::vector<SnapshotDiff> expectedDiffs = {
        { faabric::util::SnapshotDataType::Raw,
          faabric::util::SnapshotMergeOperation::Overwrite,
          expectedDiffStart,
          { sharedMem.get() + expectedDiffStart, expectedDiffSize } },
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
    }

    SECTION("From span")
    {
        snap = std::make_shared<SnapshotData>(
          std::span<uint8_t>(data.data(), data.size()));
    }

    SECTION("From vector") { snap = std::make_shared<SnapshotData>(data); }

    REQUIRE(snap->getSize() == data.size());
    REQUIRE(snap->getMaxSize() == expectedMaxSize);

    std::vector<uint8_t> actualCopy = snap->getDataCopy();
    REQUIRE(actualCopy == data);

    std::vector<uint8_t> actualChunk =
      snap->getDataCopy(chunkOffset, chunk.size());
    REQUIRE(actualChunk == chunk);

    const std::vector<uint8_t> actualConst(
      snap->getDataPtr(), snap->getDataPtr() + snap->getSize());
    REQUIRE(actualConst == data);
}

TEST_CASE("Test snapshot mapped memory diffs", "[snapshot][util]")
{
    int nSnapPages = 5;
    size_t snapSize = nSnapPages * HOST_PAGE_SIZE;

    auto snap = std::make_shared<SnapshotData>(snapSize);

    std::vector<uint8_t> dataA(150, 3);
    std::vector<uint8_t> dataB(200, 4);
    std::vector<uint8_t> dataC(250, 5);

    uint32_t offsetA = 0;
    uint32_t offsetB = HOST_PAGE_SIZE;
    uint32_t offsetC = HOST_PAGE_SIZE + 1 + dataB.size();

    // Add merge regions
    snap->addMergeRegion(offsetA,
                         dataA.size(),
                         SnapshotDataType::Raw,
                         SnapshotMergeOperation::Overwrite);

    snap->addMergeRegion(offsetB,
                         dataB.size(),
                         SnapshotDataType::Raw,
                         SnapshotMergeOperation::Overwrite);

    snap->addMergeRegion(offsetC,
                         dataC.size(),
                         SnapshotDataType::Raw,
                         SnapshotMergeOperation::Overwrite);

    // Write data to snapshot
    snap->copyInData(dataA);

    // Map some memory
    MemoryRegion memA = allocatePrivateMemory(snapSize);
    snap->mapToMemory({ memA.get(), snapSize });

    faabric::util::resetDirtyTracking();

    std::vector<uint8_t> actualSnap = snap->getDataCopy();
    std::vector<uint8_t> actualA(memA.get(), memA.get() + snapSize);
    REQUIRE(actualSnap == actualA);

    // Write data to snapshot
    snap->copyInData(dataB, offsetB);

    // Write data to memory
    std::memcpy(memA.get() + offsetC, dataC.data(), dataC.size());

    // Check diffs from memory vs snapshot
    std::vector<SnapshotDiff> actualDiffs =
      MemoryView({ memA.get(), snapSize }).diffWithSnapshot(snap);
    REQUIRE(actualDiffs.size() == 1);

    SnapshotDiff& actualDiff = actualDiffs.at(0);
    REQUIRE(actualDiff.getData().size() == dataC.size());
    REQUIRE(actualDiff.getOffset() == offsetC);

    // Apply diffs from memory to the snapshot
    snap->queueDiffs(actualDiffs);
    snap->writeQueuedDiffs();

    // Check snapshot now shows modified page
    std::vector<SnapshotDiff> snapDirtyRegions =
      MemoryView({ snap->getDataPtr(), snap->getSize() }).getDirtyRegions();

    REQUIRE(snapDirtyRegions.size() == 1);
    SnapshotDiff& snapDirtyRegion = snapDirtyRegions.at(0);
    REQUIRE(snapDirtyRegion.getOffset() == HOST_PAGE_SIZE);

    // Check modified data includes both updates
    std::vector<uint8_t> dirtyRegionData = snapDirtyRegion.getDataCopy();
    REQUIRE(dirtyRegionData.size() == HOST_PAGE_SIZE);

    std::vector<uint8_t> expectedDirtyRegionData(HOST_PAGE_SIZE, 0);
    std::memcpy(expectedDirtyRegionData.data() + (offsetB - HOST_PAGE_SIZE),
                dataB.data(),
                dataB.size());
    std::memcpy(expectedDirtyRegionData.data() + (offsetC - HOST_PAGE_SIZE),
                dataC.data(),
                dataC.size());

    REQUIRE(dirtyRegionData == expectedDirtyRegionData);

    // Map more memory from the snapshot, check it contains all updates
    MemoryRegion memB = allocatePrivateMemory(snapSize);
    snap->mapToMemory({ memB.get(), snapSize });
    std::vector<uint8_t> expectedFinal(snapSize, 0);
    std::memcpy(expectedFinal.data() + offsetA, dataA.data(), dataA.size());
    std::memcpy(expectedFinal.data() + offsetB, dataB.data(), dataB.size());
    std::memcpy(expectedFinal.data() + offsetC, dataC.data(), dataC.size());

    std::vector<uint8_t> actualMemB(memB.get(), memB.get() + snapSize);
    REQUIRE(actualMemB == expectedFinal);

    // Remap first memory and check this also contains all updates
    snap->mapToMemory({ memA.get(), snapSize });
    std::vector<uint8_t> remappedMemA(memB.get(), memB.get() + snapSize);
    REQUIRE(remappedMemA == expectedFinal);
}
}
