#include <catch2/catch.hpp>

#include "faabric_utils.h"

#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/util/dirty.h>
#include <faabric/util/memory.h>

using namespace faabric::snapshot;
using namespace faabric::util;

namespace tests {

void checkSnapshotDiff(int offset,
                       std::vector<uint8_t> data,
                       SnapshotDiff& actual)
{
    REQUIRE(offset == actual.getOffset());
    REQUIRE(!actual.getData().empty());
    REQUIRE(actual.getData().data() != nullptr);

    std::vector<uint8_t> actualData(actual.getData().begin(),
                                    actual.getData().end());
    REQUIRE(data == actualData);
}

TEST_CASE_METHOD(SnapshotTestFixture,
                 "Test single extension diff if no merge regions and grown",
                 "[snapshot]")
{
    std::string snapKey = "foobar123";
    int snapPages = 5;

    size_t snapSize = snapPages * faabric::util::HOST_PAGE_SIZE;
    auto snap = std::make_shared<SnapshotData>(snapSize);
    reg.registerSnapshot(snapKey, snap);

    int memPages = 8;
    size_t memSize = memPages * HOST_PAGE_SIZE;
    MemoryRegion mem = allocatePrivateMemory(memSize);
    std::span<uint8_t> memView(mem.get(), memSize);

    // Check we can write to shared mem
    mem[0] = 1;

    // Map to the snapshot
    snap->mapToMemory({ mem.get(), snapSize });

    // Track changes
    DirtyTracker& tracker = getDirtyTracker();
    tracker.startTracking(memView);
    tracker.startThreadLocalTracking(memView);

    std::vector<char> expected(memPages, 0);

    // Make various changes
    mem[0] = 1;
    mem[2 * HOST_PAGE_SIZE] = 1;
    mem[3 * HOST_PAGE_SIZE + 10] = 1;

    // Outside of original snap size
    mem[8 * HOST_PAGE_SIZE - 20] = 1;

    expected[0] = 1;
    expected[2] = 1;
    expected[3] = 1;
    expected[7] = 1;

    tracker.stopTracking(memView);
    tracker.stopThreadLocalTracking(memView);

    // Check there are no diffs even though we have dirty regions
    auto dirtyRegions = tracker.getBothDirtyPages(memView);
    REQUIRE(dirtyRegions == expected);

    std::vector<SnapshotDiff> changeDiffs =
      snap->diffWithDirtyRegions(memView, dirtyRegions);
    REQUIRE(changeDiffs.size() == 1);

    SnapshotDiff actual = changeDiffs.at(0);
    REQUIRE(actual.getOffset() == snapSize);

    std::span<uint8_t> expectedData =
      memView.subspan(snapSize, 3 * HOST_PAGE_SIZE);
    REQUIRE(actual.getData().size() == expectedData.size());
}

TEST_CASE_METHOD(SnapshotTestFixture, "Test snapshot diffs", "[snapshot]")
{
    std::string snapKey = "foobar123";
    int snapPages = 5;
    size_t snapSize = snapPages * HOST_PAGE_SIZE;

    auto snap = std::make_shared<SnapshotData>(snapSize);
    reg.registerSnapshot(snapKey, snap);

    // Make shared memory larger than original snapshot
    int memPages = 8;
    size_t memSize = memPages * HOST_PAGE_SIZE;
    MemoryRegion mem = allocatePrivateMemory(memSize);
    std::span<uint8_t> memView(mem.get(), memSize);

    // Map the snapshot to the start of the memory
    snap->mapToMemory({ mem.get(), snapSize });

    // Reset dirty tracking
    faabric::util::DirtyTracker& tracker = faabric::util::getDirtyTracker();
    tracker.clearAll();
    tracker.startTracking(memView);
    tracker.startThreadLocalTracking(memView);

    // Single change, single merge region
    std::vector<uint8_t> dataA = { 1, 2, 3, 4 };
    int offsetA = HOST_PAGE_SIZE;
    std::memcpy(mem.get() + offsetA, dataA.data(), dataA.size());

    snap->addMergeRegion(offsetA,
                         dataA.size(),
                         SnapshotDataType::Raw,
                         SnapshotMergeOperation::Overwrite);

    // Deliberately add merge regions out of order

    // Merge region across page boudary, capturing only part of a change
    std::vector<uint8_t> dataC = { 7, 6, 5, 4, 3, 2, 1 };
    std::vector<uint8_t> expectedDataC = { 7, 6, 5, 4 };
    int offsetC = 2 * HOST_PAGE_SIZE;
    std::memcpy(mem.get() + offsetC, dataC.data(), dataC.size());

    int regionOffsetC = offsetC - 3;
    snap->addMergeRegion(regionOffsetC,
                         dataC.size(),
                         SnapshotDataType::Raw,
                         SnapshotMergeOperation::Overwrite);

    // Two changes in single merge region
    std::vector<uint8_t> dataB1 = { 4, 5, 6 };
    std::vector<uint8_t> dataB2 = { 7, 6, 5 };
    int offsetB1 = HOST_PAGE_SIZE + 10;
    int offsetB2 = HOST_PAGE_SIZE + 16;
    std::memcpy(mem.get() + offsetB1, dataB1.data(), dataB1.size());
    std::memcpy(mem.get() + offsetB2, dataB2.data(), dataB2.size());

    snap->addMergeRegion(offsetB1,
                         (offsetB2 - offsetB1) + dataB2.size() + 10,
                         SnapshotDataType::Raw,
                         SnapshotMergeOperation::Overwrite);

    // Merge region within a change
    std::vector<uint8_t> dataD = { 1, 1, 2, 2, 3, 3, 4 };
    std::vector<uint8_t> expectedDataD = { 2, 2, 3 };
    int offsetD = 3 * HOST_PAGE_SIZE - dataD.size();
    std::memcpy(mem.get() + offsetD, dataD.data(), dataD.size());

    int regionOffsetD = offsetD + 2;
    int regionSizeD = dataD.size() - 4;
    snap->addMergeRegion(regionOffsetD,
                         regionSizeD,
                         SnapshotDataType::Raw,
                         SnapshotMergeOperation::Overwrite);

    // Write some data to the region that exceeds the size of the original.
    // Anything outside the original snapshot should be marked as changed.
    std::vector<uint8_t> dataExtra = { 2, 2, 2 };
    uint32_t extensionPages = memPages - snapPages;
    std::vector<uint8_t> expectedDataExtra(extensionPages * HOST_PAGE_SIZE, 0);
    int extraOffset = snapSize + HOST_PAGE_SIZE + 10;

    // Copy data into place in the original memory, and update the expectation
    std::memcpy(mem.get() + extraOffset, dataExtra.data(), dataExtra.size());
    std::memcpy(expectedDataExtra.data() + HOST_PAGE_SIZE + 10,
                dataExtra.data(),
                dataExtra.size());

    // Include an offset which doesn't change the data, but will register
    // a dirty page (do this by writing bytes from the original)
    int offsetNoChange = 4 * HOST_PAGE_SIZE - 10;
    std::vector<uint8_t> dataNoChange(mem.get() + offsetNoChange,
                                      mem.get() + offsetNoChange + 5);
    std::memcpy(
      mem.get() + offsetNoChange, dataNoChange.data(), dataNoChange.size());

    // Stop tracking
    tracker.stopTracking(memView);
    tracker.stopThreadLocalTracking(memView);

    // Check we have the right number of diffs
    auto dirtyRegions = tracker.getBothDirtyPages(memView);
    std::vector<SnapshotDiff> changeDiffs =
      snap->diffWithDirtyRegions(memView, dirtyRegions);

    REQUIRE(changeDiffs.size() == 6);

    // Diffs are returned increasing order of offset
    checkSnapshotDiff(snapSize, expectedDataExtra, changeDiffs.at(0));
    checkSnapshotDiff(offsetA, dataA, changeDiffs.at(1));
    checkSnapshotDiff(offsetB1, dataB1, changeDiffs.at(2));
    checkSnapshotDiff(offsetB2, dataB2, changeDiffs.at(3));
    checkSnapshotDiff(offsetC, expectedDataC, changeDiffs.at(4));
    checkSnapshotDiff(regionOffsetD, expectedDataD, changeDiffs.at(5));
}
}
