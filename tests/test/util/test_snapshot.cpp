#include <catch2/catch.hpp>

#include "faabric_utils.h"
#include "fixtures.h"

#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/util/bytes.h>
#include <faabric/util/config.h>
#include <faabric/util/dirty.h>
#include <faabric/util/macros.h>
#include <faabric/util/memory.h>
#include <faabric/util/snapshot.h>

using namespace faabric::util;

namespace tests {

class SnapshotMergeTestFixture
  : public ConfTestFixture
  , public SnapshotTestFixture
{
  public:
    SnapshotMergeTestFixture() = default;

    ~SnapshotMergeTestFixture() = default;

  protected:
    std::string snapKey = "foobar123";

    std::shared_ptr<SnapshotData> setUpSnapshot(int snapPages, int memPages)

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
      SnapshotDataType::Raw, SnapshotMergeOperation::Bytewise, offsetA, dataA);

    SnapshotDiff diffB(SnapshotDataType::Raw,
                       SnapshotMergeOperation::Bytewise,
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

    REQUIRE(diffA.getOperation() == SnapshotMergeOperation::Bytewise);
    REQUIRE(diffB.getOperation() == SnapshotMergeOperation::Bytewise);
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
    int memPages = 20;
    size_t memSize = memPages * HOST_PAGE_SIZE;
    MemoryRegion mem = allocatePrivateMemory(memSize);

    // Check it's zeroed
    std::vector<uint8_t> expectedInitial(snap->getSize(), 0);
    std::vector<uint8_t> actualSharedMemBefore(mem.get(),
                                               mem.get() + snap->getSize());
    REQUIRE(actualSharedMemBefore == expectedInitial);

    // Map the snapshot and check again
    snap->mapToMemory({ mem.get(), snap->getSize() });
    std::vector<uint8_t> actualSharedMemAfter(mem.get(),
                                              mem.get() + snap->getSize());
    REQUIRE(actualSharedMemAfter == actualSnapMem);
}

TEST_CASE_METHOD(SnapshotMergeTestFixture,
                 "Test mapping editing and remapping memory",
                 "[snapshot][util]")
{
    SECTION("Soft PTEs") { conf.dirtyTrackingMode = "softpte"; }

    SECTION("Segfaults") { conf.dirtyTrackingMode = "segfault"; }

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
    MemoryRegion memA = allocatePrivateMemory(snapSize);
    MemoryRegion memB = allocatePrivateMemory(snapSize);

    // Map the snapshot and both regions reflect the change
    std::span<uint8_t> memViewA = { memA.get(), snapSize };
    std::span<uint8_t> memViewB = { memB.get(), snapSize };
    snap->mapToMemory(memViewA);
    snap->mapToMemory(memViewB);

    REQUIRE(std::vector(memA.get(), memA.get() + snapSize) == expectedSnapMem);
    REQUIRE(std::vector(memB.get(), memB.get() + snapSize) == expectedSnapMem);

    // Make different edits to both mapped regions, check they are not
    // propagated back to the snapshot
    std::memcpy(memA.get() + offsetA, dataA.data(), dataA.size());
    std::memcpy(memB.get() + offsetB, dataB.data(), dataB.size());

    std::vector<uint8_t> actualSnapMem = snap->getDataCopy();
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
    MemoryRegion mem = allocatePrivateMemory(expandedSize);
    snap->mapToMemory({ mem.get(), originalSize });

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
    snap->mapToMemory({ mem.get(), snap->getSize() });

    // Check mapped region matches
    std::vector<uint8_t> actualData = snap->getDataCopy();
    std::vector<uint8_t> actualSharedMem(mem.get(),
                                         mem.get() + snap->getSize());

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
    MemoryRegion mem = allocatePrivateMemory(memSize);
    std::span<uint8_t> memView({ mem.get(), memSize });
    snap->mapToMemory(memView);

    // Check mapping works
    int* intA = (int*)(mem.get() + intAOffset);
    int* intB = (int*)(mem.get() + intBOffset);

    REQUIRE(*intA == originalValueA);
    REQUIRE(*intB == originalValueB);

    // Reset dirty tracking to get a clean start
    DirtyTracker& tracker = getDirtyTracker();
    tracker.clearAll();
    tracker.startTracking(memView);
    tracker.startThreadLocalTracking(memView);

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
    std::memcpy(mem.get() + otherOffset, otherData.data(), otherData.size());

    // Get the snapshot diffs
    tracker.stopTracking(memView);
    tracker.stopThreadLocalTracking(memView);
    auto dirtyRegions = tracker.getBothDirtyPages(memView);
    std::vector<SnapshotDiff> actualDiffs =
      snap->diffWithDirtyRegions(memView, dirtyRegions);

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
    size_t memSize = snapPages * HOST_PAGE_SIZE;
    MemoryRegion mem = allocatePrivateMemory(memSize);
    std::span<uint8_t> memView(mem.get(), memSize);
    snap->mapToMemory(memView);

    // Reset dirty tracking
    DirtyTracker& tracker = getDirtyTracker();
    tracker.clearAll();
    tracker.startTracking(memView);
    tracker.startThreadLocalTracking(memView);

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
    *(int*)(mem.get() + offsetA) = finalA;
    *(int*)(mem.get() + offsetB) = finalB;
    *(int*)(mem.get() + offsetC) = finalC;
    *(int*)(mem.get() + offsetD) = finalD;

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

    tracker.stopTracking(memView);
    tracker.stopThreadLocalTracking(memView);
    auto dirtyRegions = tracker.getBothDirtyPages(memView);
    std::vector<SnapshotDiff> actualDiffs =
      snap->diffWithDirtyRegions(memView, dirtyRegions);
    REQUIRE(actualDiffs.size() == 4);

    checkDiffs(actualDiffs, expectedDiffs);
}

TEST_CASE_METHOD(SnapshotMergeTestFixture,
                 "Test snapshot merge regions",
                 "[snapshot][util]")
{
    std::string snapKey = "foobar123";
    int snapPages = 5;

    std::shared_ptr<SnapshotData> snap =
      std::make_shared<SnapshotData>(snapPages * HOST_PAGE_SIZE);

    size_t memSize = snapPages * HOST_PAGE_SIZE;
    MemoryRegion mem = allocatePrivateMemory(memSize);
    std::span<uint8_t> memView(mem.get(), memSize);

    std::vector<uint8_t> originalData;
    std::vector<uint8_t> updatedData;
    std::vector<uint8_t> expectedData;

    std::vector<char> expectedDirtyPages(snapPages, 0);
    uint32_t offset = HOST_PAGE_SIZE + (10 * sizeof(int32_t));
    expectedDirtyPages[1] = 1;

    faabric::util::SnapshotDataType dataType =
      faabric::util::SnapshotDataType::Raw;
    faabric::util::SnapshotDataType expectedDataType =
      faabric::util::SnapshotDataType::Raw;
    faabric::util::SnapshotMergeOperation operation =
      faabric::util::SnapshotMergeOperation::Bytewise;

    size_t dataLength = 0;
    size_t regionLength = 0;

    bool expectNoDiff = false;

    SECTION("Integer")
    {
        int originalValue = 0;
        int finalValue = 0;
        int diffValue = 0;

        dataType = faabric::util::SnapshotDataType::Int;
        expectedDataType = faabric::util::SnapshotDataType::Int;
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
        expectedDataType = faabric::util::SnapshotDataType::Long;
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
        expectedDataType = faabric::util::SnapshotDataType::Float;
        dataLength = sizeof(float);
        regionLength = sizeof(float);

        // Imprecision in float arithmetic makes it difficult to test the
        // floating point types here unless we use integer values.
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
        expectedDataType = faabric::util::SnapshotDataType::Double;
        dataLength = sizeof(double);
        regionLength = sizeof(double);

        // Imprecision in float arithmetic makes it difficult to test
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
        expectedDataType = faabric::util::SnapshotDataType::Raw;
        dataLength = sizeof(bool);
        regionLength = sizeof(bool);

        SECTION("Bool bytewise")
        {
            originalValue = true;
            finalValue = false;
            diffValue = false;

            operation = faabric::util::SnapshotMergeOperation::Bytewise;
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

        SECTION("Bytewise")
        {
            regionLength = dataLength;
            operation = faabric::util::SnapshotMergeOperation::Bytewise;
        }

        SECTION("Bytewise unspecified length")
        {
            regionLength = 0;
            operation = faabric::util::SnapshotMergeOperation::Bytewise;
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
    snap->mapToMemory(memView);

    // Reset dirty tracking
    DirtyTracker& tracker = getDirtyTracker();
    tracker.clearAll();
    tracker.startTracking(memView);
    tracker.startThreadLocalTracking(memView);

    // Set up the merge region
    snap->addMergeRegion(offset, regionLength, dataType, operation);

    // Modify the value
    std::memcpy(mem.get() + offset, updatedData.data(), updatedData.size());

    // Get the snapshot diffs
    tracker.stopTracking(memView);
    tracker.stopThreadLocalTracking(memView);

    auto dirtyRegions = tracker.getBothDirtyPages(memView);
    REQUIRE(dirtyRegions == expectedDirtyPages);

    std::vector<SnapshotDiff> actualDiffs =
      snap->diffWithDirtyRegions(memView, dirtyRegions);

    if (expectNoDiff) {
        REQUIRE(actualDiffs.empty());
    } else {
        // Check diff
        REQUIRE(actualDiffs.size() == 1);
        std::vector<SnapshotDiff> expectedDiffs = {
            { expectedDataType,
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
      faabric::util::SnapshotMergeOperation::Bytewise;
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
    size_t memSize = snapPages * HOST_PAGE_SIZE;
    MemoryRegion mem = allocatePrivateMemory(memSize);
    std::span<uint8_t> memView(mem.get(), memSize);
    snap->mapToMemory(memView);

    // Reset dirty tracking
    DirtyTracker& tracker = getDirtyTracker();
    tracker.clearAll();
    tracker.startTracking(memView);
    tracker.startThreadLocalTracking(memView);

    // Set up the merge region
    snap->addMergeRegion(offset, dataLength, dataType, operation);

    // Modify the value
    std::vector<uint8_t> bytes(dataLength, 3);
    std::memcpy(mem.get() + offset, bytes.data(), bytes.size());

    // Check getting diffs throws an exception
    tracker.stopTracking(memView);
    tracker.stopThreadLocalTracking(memView);
    auto dirtyRegions = tracker.getBothDirtyPages(memView);
    bool failed = false;
    try {
        snap->diffWithDirtyRegions(memView, dirtyRegions);
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
    DirtyTracker& tracker = getDirtyTracker();
    tracker.clearAll();

    // Update the snapshot
    std::vector<uint8_t> dataA = { 0, 1, 2, 3 };
    std::vector<uint8_t> dataB = { 3, 4, 5 };
    uint32_t offsetA = 0;
    uint32_t offsetB = 2 * HOST_PAGE_SIZE + 1;

    snap->copyInData(dataA, offsetA);
    snap->copyInData(dataB, offsetB);

    // Check we get the expected diffs
    std::vector<SnapshotDiff> expectedDiffs = snap->getTrackedChanges();

    REQUIRE(expectedDiffs.size() == 2);

    SnapshotDiff& diffA = expectedDiffs.at(0);
    SnapshotDiff& diffB = expectedDiffs.at(1);

    REQUIRE(diffA.getData().size() == dataA.size());
    REQUIRE(diffB.getData().size() == dataB.size());

    REQUIRE(diffA.getDataCopy() == dataA);
    REQUIRE(diffB.getDataCopy() == dataB);
}

TEST_CASE_METHOD(SnapshotMergeTestFixture,
                 "Test fine-grained byte-wise diffs",
                 "[snapshot][util]")
{
    int snapPages = 3;
    size_t memSize = snapPages * HOST_PAGE_SIZE;

    std::shared_ptr<SnapshotData> snap =
      std::make_shared<SnapshotData>(snapPages * HOST_PAGE_SIZE);
    reg.registerSnapshot(snapKey, snap);

    // Map the snapshot
    MemoryRegion mem = allocatePrivateMemory(memSize);
    std::span<uint8_t> memView(mem.get(), memSize);
    snap->mapToMemory(memView);

    // Reset dirty tracking
    DirtyTracker& tracker = getDirtyTracker();
    tracker.clearAll();
    tracker.startTracking(memView);
    tracker.startThreadLocalTracking(memView);

    std::vector<char> expectedDirtyPages(snapPages, 0);

    // Add some tightly-packed changes
    uint32_t offsetA = 0;
    std::vector<uint8_t> dataA(10, 1);
    std::memcpy(mem.get() + offsetA, dataA.data(), dataA.size());
    expectedDirtyPages[0] = 1;

    uint32_t offsetB = dataA.size() + 1;
    std::vector<uint8_t> dataB(2, 1);
    std::memcpy(mem.get() + offsetB, dataB.data(), dataB.size());

    uint32_t offsetC = offsetB + 3;
    std::vector<uint8_t> dataC(1, 1);
    std::memcpy(mem.get() + offsetC, dataC.data(), dataC.size());

    uint32_t offsetD = offsetC + 2;
    std::vector<uint8_t> dataD(1, 1);
    std::memcpy(mem.get() + offsetD, dataD.data(), dataD.size());

    std::vector<SnapshotDiff> expectedDiffs = {
        { SnapshotDataType::Raw,
          SnapshotMergeOperation::Bytewise,
          offsetA,
          { dataA.data(), dataA.size() } },
        { SnapshotDataType::Raw,
          SnapshotMergeOperation::Bytewise,
          offsetB,
          { dataB.data(), dataB.size() } },
        { SnapshotDataType::Raw,
          SnapshotMergeOperation::Bytewise,
          offsetC,
          { dataC.data(), dataC.size() } },
        { SnapshotDataType::Raw,
          SnapshotMergeOperation::Bytewise,
          offsetD,
          { dataD.data(), dataD.size() } },
    };

    // Add a single merge region for all the changes
    snap->addMergeRegion(0,
                         offsetD + dataD.size() + 20,
                         SnapshotDataType::Raw,
                         SnapshotMergeOperation::Bytewise);

    // Check number of diffs
    tracker.stopTracking(memView);
    tracker.stopThreadLocalTracking(memView);

    auto dirtyRegions = tracker.getBothDirtyPages(memView);
    REQUIRE(dirtyRegions == expectedDirtyPages);

    std::vector<SnapshotDiff> actualDiffs =
      snap->diffWithDirtyRegions(memView, dirtyRegions);

    checkDiffs(actualDiffs, expectedDiffs);
}

void doFillGapsChecks(SnapshotMergeOperation op)
{
    int snapPages = 3;
    size_t snapSize = snapPages * HOST_PAGE_SIZE;
    auto snap = std::make_shared<SnapshotData>(snapSize);

    std::vector<SnapshotMergeRegion> expectedRegions;

    SECTION("No existing regions")
    {
        expectedRegions.emplace_back(0, 0, SnapshotDataType::Raw, op);
    }

    SECTION("One region at start")
    {
        snap->addMergeRegion(
          0, 100, SnapshotDataType::Raw, SnapshotMergeOperation::Bytewise);

        expectedRegions.emplace_back(
          0, 100, SnapshotDataType::Raw, SnapshotMergeOperation::Bytewise);

        expectedRegions.emplace_back(100, 0, SnapshotDataType::Raw, op);
    }

    SECTION("One region at end")
    {
        snap->addMergeRegion(snapSize - 100,
                             100,
                             SnapshotDataType::Raw,
                             SnapshotMergeOperation::Bytewise);

        expectedRegions.emplace_back(
          0, snapSize - 100, SnapshotDataType::Raw, op);

        expectedRegions.emplace_back((uint32_t)snapSize - 100,
                                     100,
                                     SnapshotDataType::Raw,
                                     SnapshotMergeOperation::Bytewise);
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
                             SnapshotMergeOperation::Bytewise);

        expectedRegions.emplace_back(0, 100, SnapshotDataType::Raw, op);

        expectedRegions.emplace_back(
          100, sizeof(int), SnapshotDataType::Int, SnapshotMergeOperation::Sum);

        expectedRegions.emplace_back(100 + sizeof(int),
                                     HOST_PAGE_SIZE - (100 + sizeof(int)),
                                     SnapshotDataType::Raw,
                                     op);

        expectedRegions.emplace_back((uint32_t)HOST_PAGE_SIZE,
                                     sizeof(double),
                                     SnapshotDataType::Double,
                                     SnapshotMergeOperation::Product);

        expectedRegions.emplace_back(
          (uint32_t)(HOST_PAGE_SIZE + sizeof(double)),
          200 - sizeof(double),
          SnapshotDataType::Raw,
          op);

        expectedRegions.emplace_back((uint32_t)HOST_PAGE_SIZE + 200,
                                     (uint32_t)HOST_PAGE_SIZE,
                                     SnapshotDataType::Raw,
                                     SnapshotMergeOperation::Bytewise);

        expectedRegions.emplace_back(
          (uint32_t)(2 * HOST_PAGE_SIZE) + 200, 0, SnapshotDataType::Raw, op);
    }

    snap->fillGapsWithBytewiseRegions();

    std::vector<SnapshotMergeRegion> actualRegions = snap->getMergeRegions();

    // Sort regions to ensure consistent comparison
    std::sort(actualRegions.begin(), actualRegions.end());

    REQUIRE(actualRegions.size() == expectedRegions.size());
    for (int i = 0; i < actualRegions.size(); i++) {
        SnapshotMergeRegion expectedRegion = expectedRegions[i];
        SnapshotMergeRegion actualRegion = actualRegions[i];
        REQUIRE(actualRegion == expectedRegion);
    }
}

TEST_CASE_METHOD(SnapshotMergeTestFixture,
                 "Test filling gaps in regions",
                 "[snapshot][util]")
{
    SystemConfig& conf = getSystemConfig();
    SECTION("Bytewise")
    {
        conf.diffingMode = "bytewise";
        doFillGapsChecks(SnapshotMergeOperation::Bytewise);
    }

    SECTION("XOR")
    {
        conf.diffingMode = "xor";
        doFillGapsChecks(SnapshotMergeOperation::XOR);
    }

    SECTION("Unsupported")
    {
        conf.diffingMode = "foobar";

        int snapPages = 3;
        size_t snapSize = snapPages * HOST_PAGE_SIZE;
        auto snap = std::make_shared<SnapshotData>(snapSize);

        bool failed = false;
        try {
            snap->fillGapsWithBytewiseRegions();
        } catch (std::runtime_error& ex) {
            std::string expected = "Unsupported diffing mode";
            REQUIRE(ex.what() == expected);
            failed = true;
        }

        REQUIRE(failed);
    }
}

TEST_CASE("Test sorting snapshot merge regions", "[snapshot][util]")
{
    std::vector<SnapshotMergeRegion> regions;

    regions.emplace_back(
      10, 20, SnapshotDataType::Double, SnapshotMergeOperation::Sum);
    regions.emplace_back(
      50, 20, SnapshotDataType::Double, SnapshotMergeOperation::Sum);
    regions.emplace_back(
      5, 20, SnapshotDataType::Raw, SnapshotMergeOperation::Bytewise);
    regions.emplace_back(
      30, 20, SnapshotDataType::Float, SnapshotMergeOperation::Max);

    std::sort(regions.begin(), regions.end());

    std::vector<SnapshotMergeRegion> expected = {
        { 5, 20, SnapshotDataType::Raw, SnapshotMergeOperation::Bytewise },
        { 10, 20, SnapshotDataType::Double, SnapshotMergeOperation::Sum },
        { 30, 20, SnapshotDataType::Float, SnapshotMergeOperation::Max },
        { 50, 20, SnapshotDataType::Double, SnapshotMergeOperation::Sum },
    };

    REQUIRE(regions == expected);
}

TEST_CASE_METHOD(SnapshotMergeTestFixture,
                 "Test mix of applicable and non-applicable merge regions",
                 "[snapshot][util]")
{
    int snapPages = 6;
    size_t memSize = snapPages * HOST_PAGE_SIZE;

    std::shared_ptr<SnapshotData> snap =
      std::make_shared<SnapshotData>(snapPages * HOST_PAGE_SIZE);
    reg.registerSnapshot(snapKey, snap);

    // Map the snapshot
    MemoryRegion mem = allocatePrivateMemory(memSize);
    std::span<uint8_t> memView(mem.get(), memSize);
    snap->mapToMemory(memView);

    // Reset dirty tracking
    DirtyTracker& tracker = getDirtyTracker();
    tracker.clearAll();
    tracker.startTracking(memView);
    tracker.startThreadLocalTracking(memView);

    std::vector<char> expectedDirtyPages(snapPages, 0);

    // Add a couple of merge regions on each page, which should be skipped as
    // they won't overlap any changes
    for (int i = 0; i < snapPages; i++) {
        // Bytewise region at bottom of page
        int skippedBytewiseOffset = i * HOST_PAGE_SIZE;
        snap->addMergeRegion(skippedBytewiseOffset,
                             10,
                             faabric::util::SnapshotDataType::Raw,
                             faabric::util::SnapshotMergeOperation::Bytewise);

        // Sum region near top of page
        int skippedSumOffset =
          ((i + 1) * HOST_PAGE_SIZE) - (2 * sizeof(int32_t));
        snap->addMergeRegion(skippedSumOffset,
                             sizeof(int32_t),
                             faabric::util::SnapshotDataType::Int,
                             faabric::util::SnapshotMergeOperation::Sum);
    }

    // Add a bytewise region above the one at the very bottom, and some
    // modified data that should be caught by it
    uint32_t bytewiseAOffset = (2 * HOST_PAGE_SIZE) + 20;
    snap->addMergeRegion(bytewiseAOffset,
                         20,
                         faabric::util::SnapshotDataType::Raw,
                         faabric::util::SnapshotMergeOperation::Bytewise);

    std::vector<uint8_t> bytewiseData(10, 1);
    SPDLOG_DEBUG(
      "Mix test writing {} bytes at {}", bytewiseData.size(), bytewiseAOffset);
    std::memcpy(
      mem.get() + bytewiseAOffset, bytewiseData.data(), bytewiseData.size());
    expectedDirtyPages[2] = 1;

    // Add a sum region and modified data that should also cause a diff
    uint32_t sumOffset = (4 * HOST_PAGE_SIZE) + 100;
    int sumValue = 333;
    int sumOriginal = 111;
    int sumExpected = 222;

    snap->addMergeRegion(sumOffset,
                         sizeof(int32_t),
                         faabric::util::SnapshotDataType::Int,
                         faabric::util::SnapshotMergeOperation::Sum);

    SPDLOG_DEBUG("Mix test writing int at {}", sumOffset);
    snap->copyInData({ BYTES(&sumOriginal), sizeof(int) }, sumOffset);
    *(int*)(mem.get() + sumOffset) = sumValue;
    expectedDirtyPages[4] = 1;

    // Check diffs
    std::vector<SnapshotDiff> expectedDiffs = {
        { faabric::util::SnapshotDataType::Raw,
          faabric::util::SnapshotMergeOperation::Bytewise,
          bytewiseAOffset,
          { BYTES(bytewiseData.data()), bytewiseData.size() } },
        { faabric::util::SnapshotDataType::Int,
          faabric::util::SnapshotMergeOperation::Sum,
          sumOffset,
          { BYTES(&sumExpected), sizeof(int32_t) } },
    };

    tracker.stopTracking(memView);
    tracker.stopThreadLocalTracking(memView);

    auto dirtyRegions = tracker.getBothDirtyPages(memView);
    REQUIRE(dirtyRegions == expectedDirtyPages);

    std::vector<SnapshotDiff> actualDiffs =
      snap->diffWithDirtyRegions(memView, dirtyRegions);

    checkDiffs(actualDiffs, expectedDiffs);
}

TEST_CASE_METHOD(SnapshotMergeTestFixture,
                 "Test diffing memory larger than snapshot",
                 "[snapshot][util]")
{
    int snapPages = 6;
    int memPages = 10;

    size_t snapSize = snapPages * HOST_PAGE_SIZE;
    size_t memSize = memPages * HOST_PAGE_SIZE;

    std::shared_ptr<SnapshotData> snap =
      std::make_shared<SnapshotData>(snapSize);
    reg.registerSnapshot(snapKey, snap);

    // Map the snapshot
    MemoryRegion mem = allocatePrivateMemory(memSize);
    std::span<uint8_t> memView(mem.get(), memSize);

    // Map only the size of the snapshot
    snap->mapToMemory({ mem.get(), snapSize });

    DirtyTracker& tracker = getDirtyTracker();
    tracker.clearAll();
    tracker.startTracking(memView);
    tracker.startThreadLocalTracking(memView);

    uint32_t changeOffset = 0;
    uint32_t mergeRegionStart = snapSize;
    size_t changeLength = 123;

    std::vector<uint8_t> overshootData((memPages - snapPages) * HOST_PAGE_SIZE,
                                       0);

    // When memory has changed at or past the end of the original data, the diff
    // will start at the end of the original data and round up to the next page
    // boundary. If the change starts before the end, it will start at the
    // beginning of the change and continue into the page boundary past the
    // original data.

    std::vector<SnapshotDiff> expectedDiffs;

    std::vector<uint8_t> zeroedPage(HOST_PAGE_SIZE, 0);
    std::vector<uint8_t> diffData(changeLength, 2);

    SECTION("Change on first page past end of original data, overlapping merge "
            "region")
    {
        changeOffset = snapSize + 100;
        mergeRegionStart = snapSize;

        diffData = std::vector<uint8_t>(100, 2);
        std::vector<uint8_t> expectedData = overshootData;
        std::memset(expectedData.data() + 100, 2, 100);

        // Diff should just be the whole of the updated memory
        expectedDiffs = { { faabric::util::SnapshotDataType::Raw,
                            faabric::util::SnapshotMergeOperation::Bytewise,
                            (uint32_t)snapSize,
                            expectedData } };
    }

    SECTION("Change and merge region aligned at end of original data")
    {
        changeOffset = snapSize;
        mergeRegionStart = snapSize;

        diffData = std::vector<uint8_t>(100, 2);
        std::vector<uint8_t> expectedData = overshootData;
        std::memset(expectedData.data(), 2, 100);

        // Diff should just be the whole of the updated memory
        expectedDiffs = { { faabric::util::SnapshotDataType::Raw,
                            faabric::util::SnapshotMergeOperation::Bytewise,
                            (uint32_t)snapSize,
                            expectedData } };
    }

    SECTION("Change and merge region after end of original data")
    {
        uint32_t start = (snapPages + 2) * HOST_PAGE_SIZE;
        changeOffset = start + 100;
        mergeRegionStart = start;

        diffData = std::vector<uint8_t>(100, 2);
        std::vector<uint8_t> expectedData = overshootData;
        std::memset(expectedData.data() + (2 * HOST_PAGE_SIZE) + 100, 2, 100);

        // Diff should be the whole region of updated memory
        expectedDiffs = { { faabric::util::SnapshotDataType::Raw,
                            faabric::util::SnapshotMergeOperation::Bytewise,
                            (uint32_t)snapSize,
                            expectedData } };
    }

    SECTION("Merge region and change both crossing end of original data")
    {
        // Start change one page below the end
        uint32_t start = (snapPages - 1) * HOST_PAGE_SIZE;
        changeOffset = start + 100;

        // Add a merge region two pages below
        mergeRegionStart = (snapPages - 2) * HOST_PAGE_SIZE;

        // Change will capture the modified bytes within the original region,
        // and the overshoot
        size_t dataSize = 2 * HOST_PAGE_SIZE;
        diffData = std::vector<uint8_t>(dataSize, 2);

        size_t overlapSize = HOST_PAGE_SIZE - 100;
        size_t overshootSize = dataSize - overlapSize;

        // One diff will cover the overlap with last part of original data, and
        // another will be the rest of the data
        std::vector<uint8_t> expectedDataOne(HOST_PAGE_SIZE - 100, 2);
        std::vector<uint8_t> expectedDataTwo = overshootData;
        std::memset(expectedDataTwo.data(), 2, overshootSize);

        expectedDiffs = { { faabric::util::SnapshotDataType::Raw,
                            faabric::util::SnapshotMergeOperation::Bytewise,
                            (uint32_t)snapSize,
                            expectedDataTwo },
                          { faabric::util::SnapshotDataType::Raw,
                            faabric::util::SnapshotMergeOperation::Bytewise,
                            changeOffset,
                            expectedDataOne } };
    }

    // Copy in the changed data
    std::memcpy(mem.get() + changeOffset, diffData.data(), diffData.size());

    // Add a merge region
    snap->addMergeRegion(mergeRegionStart,
                         0,
                         faabric::util::SnapshotDataType::Raw,
                         faabric::util::SnapshotMergeOperation::Bytewise);

    tracker.stopTracking(memView);
    tracker.stopThreadLocalTracking(memView);
    auto dirtyRegions = tracker.getBothDirtyPages(memView);

    std::vector<SnapshotDiff> actualDiffs =
      snap->diffWithDirtyRegions(memView, dirtyRegions);

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

TEST_CASE_METHOD(DirtyTrackingTestFixture,
                 "Test snapshot mapped memory diffs",
                 "[snapshot][util]")
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
                         SnapshotMergeOperation::Bytewise);

    snap->addMergeRegion(offsetB,
                         dataB.size(),
                         SnapshotDataType::Raw,
                         SnapshotMergeOperation::Bytewise);

    snap->addMergeRegion(offsetC,
                         dataC.size(),
                         SnapshotDataType::Raw,
                         SnapshotMergeOperation::Bytewise);

    // Write some initial data to snapshot
    snap->copyInData(dataA);

    // Map some memory
    MemoryRegion memA = allocatePrivateMemory(snapSize);
    std::span<uint8_t> memViewA(memA.get(), snapSize);
    snap->mapToMemory(memViewA);

    // Clear tracking
    DirtyTracker& tracker = getDirtyTracker();
    tracker.clearAll();
    snap->clearTrackedChanges();

    std::vector<uint8_t> actualSnap = snap->getDataCopy();
    std::vector<uint8_t> actualMemA(memA.get(), memA.get() + snapSize);
    REQUIRE(actualSnap == actualMemA);

    // Start dirty tracking
    tracker.startTracking(memViewA);
    tracker.startThreadLocalTracking(memViewA);

    // Write some data to the snapshot
    snap->copyInData(dataB, offsetB);

    // Write data to the mapped memory
    std::memcpy(memA.get() + offsetC, dataC.data(), dataC.size());

    // Check diff of snapshot with memory only includes the change made to
    // the memory itself
    tracker.stopTracking(memViewA);
    tracker.stopThreadLocalTracking(memViewA);
    auto dirtyRegions = tracker.getBothDirtyPages(memViewA);

    {
        std::vector<SnapshotDiff> actualDiffs =
          snap->diffWithDirtyRegions(memViewA, dirtyRegions);
        REQUIRE(actualDiffs.size() == 1);

        SnapshotDiff& actualDiff = actualDiffs.at(0);
        REQUIRE(actualDiff.getData().size() == dataC.size());
        REQUIRE(actualDiff.getOffset() == offsetC);

        // Apply diffs from memory to the snapshot
        snap->queueDiffs(actualDiffs);
        snap->writeQueuedDiffs();
    }

    // Check snapshot now shows both diffs
    std::vector<SnapshotDiff> snapDirtyRegions = snap->getTrackedChanges();

    REQUIRE(snapDirtyRegions.size() == 2);

    SnapshotDiff& diffB = snapDirtyRegions.at(0);
    SnapshotDiff& diffC = snapDirtyRegions.at(1);

    REQUIRE(diffB.getOffset() == offsetB);
    REQUIRE(diffC.getOffset() == offsetC);

    // Check modified data includes both updates
    std::vector<uint8_t> diffDataB = diffB.getDataCopy();
    std::vector<uint8_t> diffDataC = diffC.getDataCopy();

    REQUIRE(diffDataB == dataB);
    REQUIRE(diffDataC == dataC);

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

TEST_CASE("Test diffing byte array regions", "[util][snapshot]")
{
    std::vector<uint8_t> a;
    std::vector<uint8_t> b;
    std::vector<std::pair<uint32_t, uint32_t>> expected;

    uint32_t startOffset = 0;
    uint32_t endOffset = 0;
    SECTION("Equal")
    {
        a = { 0, 1, 2, 3 };
        b = { 0, 1, 2, 3 };
        startOffset = 0;
        endOffset = b.size();
    }

    SECTION("Empty") {}

    SECTION("Not equal")
    {
        a = { 0, 0, 2, 2, 3, 3, 4, 4, 5, 5 };
        b = { 0, 1, 1, 2, 3, 6, 6, 6, 5, 5 };
        startOffset = 0;
        endOffset = b.size();
        expected = {
            { 1, 2 },
            { 5, 3 },
        };
    }

    SECTION("Not equal subsections")
    {
        a = { 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0 };
        b = { 1, 1, 1, 1, 1, 0, 0, 1, 1, 1, 1, 1, 0, 0 };
        startOffset = 3;
        endOffset = 10;
        expected = {
            { 3, 2 },
            { 6, 1 },
            { 8, 2 },
        };
    }

    SECTION("Single length")
    {
        a = { 0, 1, 2, 3, 4 };
        b = { 0, 1, 3, 3, 4 };
        startOffset = 0;
        endOffset = b.size();
        expected = { { 2, 1 } };
    }

    SECTION("Difference at start")
    {
        a = { 0, 1, 2, 3, 4, 5, 6 };
        b = { 1, 2, 3, 3, 3, 4, 6 };
        startOffset = 0;
        endOffset = b.size();
        expected = { { 0, 3 }, { 4, 2 } };
    }

    SECTION("Difference at end")
    {
        a = { 0, 1, 2, 3, 4, 5, 6 };
        b = { 0, 1, 1, 3, 3, 4, 5 };
        startOffset = 0;
        endOffset = b.size();
        expected = { { 2, 1 }, { 4, 3 } };
    }

    std::vector<SnapshotDiff> actual;
    diffArrayRegions(actual, startOffset, endOffset, a, b);

    // Convert execpted into diffs
    std::vector<SnapshotDiff> expectedDiffs;
    for (auto p : expected) {
        expectedDiffs.emplace_back(
          SnapshotDataType::Raw,
          SnapshotMergeOperation::Bytewise,
          p.first,
          std::span<uint8_t>(b.data() + p.first,
                             b.data() + p.first + p.second));
    }

    REQUIRE(actual.size() == expected.size());
    for (int i = 0; i < actual.size(); i++) {
        REQUIRE(actual.at(i).getOffset() == expectedDiffs.at(i).getOffset());
        REQUIRE(actual.at(i).getDataCopy() ==
                expectedDiffs.at(i).getDataCopy());
        REQUIRE(actual.at(i).getDataType() ==
                expectedDiffs.at(i).getDataType());
        REQUIRE(actual.at(i).getOperation() ==
                expectedDiffs.at(i).getOperation());
    }
}

TEST_CASE("Test snapshot merge region equality", "[snapshot][util]")
{
    SECTION("Equal")
    {
        SnapshotMergeRegion a(
          10, 12, SnapshotDataType::Double, SnapshotMergeOperation::Ignore);
        SnapshotMergeRegion b(
          10, 12, SnapshotDataType::Double, SnapshotMergeOperation::Ignore);
        REQUIRE(a == b);
    }

    SECTION("Offset unequal")
    {
        SnapshotMergeRegion a(
          123, 12, SnapshotDataType::Double, SnapshotMergeOperation::Ignore);
        SnapshotMergeRegion b(
          10, 12, SnapshotDataType::Double, SnapshotMergeOperation::Ignore);
        REQUIRE(a != b);
    }

    SECTION("Length unequal")
    {
        SnapshotMergeRegion a(
          10, 22, SnapshotDataType::Double, SnapshotMergeOperation::Ignore);
        SnapshotMergeRegion b(
          10, 12, SnapshotDataType::Double, SnapshotMergeOperation::Ignore);
        REQUIRE(a != b);
    }

    SECTION("Data type unequal")
    {
        SnapshotMergeRegion a(
          10, 22, SnapshotDataType::Double, SnapshotMergeOperation::Ignore);
        SnapshotMergeRegion b(
          10, 22, SnapshotDataType::Bool, SnapshotMergeOperation::Ignore);
        REQUIRE(a != b);
    }

    SECTION("Operation unequal")
    {
        SnapshotMergeRegion a(
          10, 22, SnapshotDataType::Double, SnapshotMergeOperation::Ignore);
        SnapshotMergeRegion b(
          10, 22, SnapshotDataType::Double, SnapshotMergeOperation::Sum);
        REQUIRE(a != b);
    }
    SECTION("Default constructors equal")
    {
        SnapshotMergeRegion a;
        SnapshotMergeRegion b;
        REQUIRE(a == b);
    }
}

TEST_CASE_METHOD(SnapshotMergeTestFixture,
                 "Test diffing snapshot memory with none tracking",
                 "[snapshot][util]")
{
    conf.dirtyTrackingMode = "none";

    SECTION("XOR diffs") { conf.diffingMode = "xor"; }

    SECTION("Bytewise diffs") { conf.diffingMode = "bytewise"; }

    // Create snapshot
    int snapPages = 4;
    size_t snapSize = snapPages * HOST_PAGE_SIZE;

    std::shared_ptr<SnapshotData> snap =
      std::make_shared<SnapshotData>(snapSize);
    reg.registerSnapshot(snapKey, snap);

    // Map some memory
    MemoryRegion mem = allocatePrivateMemory(snapSize);
    std::span<uint8_t> memView(mem.get(), snapSize);
    snap->mapToMemory(memView);

    // Reset dirty tracking
    DirtyTracker& tracker = getDirtyTracker();
    REQUIRE(tracker.getType() == "none");
    tracker.clearAll();

    // Start tracking
    tracker.startTracking(memView);
    tracker.startThreadLocalTracking(memView);

    // Update the memory
    std::vector<uint8_t> dataA = { 1, 2, 3, 4 };
    std::vector<uint8_t> dataB = { 3, 4, 5 };
    uint32_t offsetA = 0;
    uint32_t offsetB = 2 * HOST_PAGE_SIZE + 1;

    std::memcpy(mem.get() + offsetA, dataA.data(), dataA.size());
    std::memcpy(mem.get() + offsetB, dataB.data(), dataB.size());

    // Stop tracking
    tracker.stopTracking(memView);
    tracker.stopThreadLocalTracking(memView);

    // Get diffs
    std::vector<char> expectedDirtyPages(snapPages, 1);
    std::vector<char> dirtyPages = tracker.getBothDirtyPages(memView);
    REQUIRE(dirtyPages == expectedDirtyPages);

    // Diff with snapshot
    snap->fillGapsWithBytewiseRegions();
    std::vector<SnapshotMergeRegion> actualRegions = snap->getMergeRegions();
    REQUIRE(actualRegions.size() == 1);
    REQUIRE(actualRegions.at(0).offset == 0);
    REQUIRE(actualRegions.at(0).length == 0);

    std::vector<faabric::util::SnapshotDiff> actual =
      snap->diffWithDirtyRegions(memView, dirtyPages);

    if (conf.diffingMode == "bytewise") {
        REQUIRE(actual.size() == 2);
        SnapshotDiff diffA = actual.at(0);
        SnapshotDiff diffB = actual.at(1);

        REQUIRE(diffA.getOffset() == offsetA);
        REQUIRE(diffB.getOffset() == offsetB);

        REQUIRE(diffA.getDataCopy() == dataA);
        REQUIRE(diffB.getDataCopy() == dataB);
    } else if (conf.diffingMode == "xor") {
        REQUIRE(actual.size() == snapPages);

        std::vector<uint8_t> blankPage(HOST_PAGE_SIZE, 0);

        SnapshotDiff diffPage1 = actual.at(0);
        SnapshotDiff diffPage2 = actual.at(1);
        SnapshotDiff diffPage3 = actual.at(2);
        SnapshotDiff diffPage4 = actual.at(3);

        REQUIRE(diffPage1.getOffset() == 0);
        REQUIRE(diffPage2.getOffset() == HOST_PAGE_SIZE);
        REQUIRE(diffPage3.getOffset() == HOST_PAGE_SIZE * 2);
        REQUIRE(diffPage4.getOffset() == HOST_PAGE_SIZE * 3);

        // Pages with no diffs will just be zeroes
        REQUIRE(diffPage2.getDataCopy() == blankPage);
        REQUIRE(diffPage4.getDataCopy() == blankPage);

        // Other pages will have diffs in place
        std::vector<uint8_t> expectedA = blankPage;
        std::vector<uint8_t> expectedB = blankPage;
        std::copy(dataA.begin(), dataA.end(), expectedA.begin());
        std::copy(dataB.begin(), dataB.end(), expectedB.begin() + 1);

        REQUIRE(diffPage1.getDataCopy() == expectedA);
        REQUIRE(diffPage3.getDataCopy() == expectedB);

    } else {
        FAIL();
    }
}

TEST_CASE_METHOD(DirtyTrackingTestFixture, "Test XOR diffs", "[util][snapshot]")
{
    int nSnapPages = 5;
    size_t snapSize = nSnapPages * HOST_PAGE_SIZE;

    auto snap = std::make_shared<SnapshotData>(snapSize);

    std::vector<char> expectedDirtyPages(nSnapPages, 0);
    std::vector<uint8_t> expectedSnapData(snapSize, 0);

    std::vector<uint8_t> dataA(150, 3);
    std::vector<uint8_t> dataB(200, 4);

    uint32_t offsetA = 0;
    uint32_t offsetB = 2 * HOST_PAGE_SIZE + 100;
    expectedDirtyPages[0] = 1;
    expectedDirtyPages[2] = 1;

    // Add merge regions
    snap->addMergeRegion(offsetA,
                         dataA.size(),
                         SnapshotDataType::Raw,
                         SnapshotMergeOperation::XOR);

    snap->addMergeRegion(offsetB,
                         dataB.size(),
                         SnapshotDataType::Raw,
                         SnapshotMergeOperation::XOR);

    // Map some memory
    MemoryRegion mem = allocatePrivateMemory(snapSize);
    std::span<uint8_t> memView(mem.get(), snapSize);
    snap->mapToMemory(memView);

    // Clear tracking
    DirtyTracker& tracker = getDirtyTracker();
    tracker.clearAll();
    snap->clearTrackedChanges();

    // Start tracking
    tracker.startTracking(memView);
    tracker.startThreadLocalTracking(memView);

    // Copy in data
    std::memcpy(mem.get() + offsetA, dataA.data(), dataA.size());
    std::memcpy(mem.get() + offsetB, dataB.data(), dataB.size());

    std::memcpy(expectedSnapData.data() + offsetA, dataA.data(), dataA.size());
    std::memcpy(expectedSnapData.data() + offsetB, dataB.data(), dataB.size());

    // Stop tracking
    tracker.stopTracking(memView);
    tracker.stopThreadLocalTracking(memView);

    // Get diffs
    std::vector<char> dirtyPages = tracker.getBothDirtyPages(memView);
    REQUIRE(dirtyPages == expectedDirtyPages);

    // Diff with snapshot
    std::vector<faabric::util::SnapshotDiff> actual =
      snap->diffWithDirtyRegions(memView, dirtyPages);

    REQUIRE(actual.size() == 2);
    SnapshotDiff diffA = actual.at(0);
    SnapshotDiff diffB = actual.at(1);

    REQUIRE(diffA.getOffset() == offsetA);
    REQUIRE(diffB.getOffset() == offsetB);

    // Apply the diffs to the snapshot
    snap->queueDiffs(actual);
    snap->writeQueuedDiffs();

    // Check snapshot data is now as expected
    REQUIRE(snap->getDataCopy() == expectedSnapData);
}
}
