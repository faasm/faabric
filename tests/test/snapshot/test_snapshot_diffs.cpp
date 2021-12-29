#include <catch2/catch.hpp>

#include "faabric_utils.h"

#include <faabric/snapshot/SnapshotRegistry.h>
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
                 "Test no snapshot diffs if no merge regions",
                 "[snapshot]")
{
    std::string snapKey = "foobar123";
    int snapPages = 5;

    size_t snapSize = snapPages * faabric::util::HOST_PAGE_SIZE;
    auto snap = std::make_shared<SnapshotData>(snapSize);
    reg.registerSnapshot(snapKey, snap);

    int sharedMemPages = 8;
    size_t sharedMemSize = sharedMemPages * HOST_PAGE_SIZE;
    MemoryRegion sharedMem = allocatePrivateMemory(sharedMemSize);

    // Check we can write to shared mem
    sharedMem[0] = 1;

    // Map to the snapshot
    snap->mapToMemory({ sharedMem.get(), snapSize });

    // Make various changes
    sharedMem[0] = 1;
    sharedMem[2 * HOST_PAGE_SIZE] = 1;
    sharedMem[3 * HOST_PAGE_SIZE + 10] = 1;
    sharedMem[8 * HOST_PAGE_SIZE - 20] = 1;

    // Check there are no diffs
    std::vector<SnapshotDiff> changeDiffs =
      MemoryView({ sharedMem.get(), sharedMemSize }).diffWithSnapshot(snap);
    REQUIRE(changeDiffs.empty());
}

TEST_CASE_METHOD(SnapshotTestFixture, "Test snapshot diffs", "[snapshot]")
{
    std::string snapKey = "foobar123";
    int snapPages = 5;
    size_t snapSize = snapPages * HOST_PAGE_SIZE;

    auto snap = std::make_shared<SnapshotData>(snapSize);
    reg.registerSnapshot(snapKey, snap);

    // Make shared memory larger than original snapshot
    int sharedMemPages = 8;
    size_t sharedMemSize = sharedMemPages * HOST_PAGE_SIZE;
    MemoryRegion sharedMem = allocatePrivateMemory(sharedMemSize);

    // Map the snapshot to the start of the memory
    snap->mapToMemory({ sharedMem.get(), snapSize });

    // Reset dirty tracking
    faabric::util::resetDirtyTracking();

    // Single change, single merge region
    std::vector<uint8_t> dataA = { 1, 2, 3, 4 };
    int offsetA = HOST_PAGE_SIZE;
    std::memcpy(sharedMem.get() + offsetA, dataA.data(), dataA.size());

    snap->addMergeRegion(offsetA,
                         dataA.size(),
                         SnapshotDataType::Raw,
                         SnapshotMergeOperation::Overwrite);

    // NOTE - deliberately add merge regions out of order
    // Diff starting in merge region and overlapping the end
    std::vector<uint8_t> dataC = { 7, 6, 5, 4, 3, 2, 1 };
    std::vector<uint8_t> expectedDataC = { 7, 6, 5, 4 };
    int offsetC = 2 * HOST_PAGE_SIZE;
    std::memcpy(sharedMem.get() + offsetC, dataC.data(), dataC.size());

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
    std::memcpy(sharedMem.get() + offsetB1, dataB1.data(), dataB1.size());
    std::memcpy(sharedMem.get() + offsetB2, dataB2.data(), dataB2.size());

    snap->addMergeRegion(offsetB1,
                         (offsetB2 - offsetB1) + dataB2.size() + 10,
                         SnapshotDataType::Raw,
                         SnapshotMergeOperation::Overwrite);

    // Merge region within a change
    std::vector<uint8_t> dataD = { 1, 1, 2, 2, 3, 3, 4 };
    std::vector<uint8_t> expectedDataD = { 2, 2, 3 };
    int offsetD = 3 * HOST_PAGE_SIZE - dataD.size();
    std::memcpy(sharedMem.get() + offsetD, dataD.data(), dataD.size());

    int regionOffsetD = offsetD + 2;
    int regionSizeD = dataD.size() - 4;
    snap->addMergeRegion(regionOffsetD,
                         regionSizeD,
                         SnapshotDataType::Raw,
                         SnapshotMergeOperation::Overwrite);

    // Write some data to the region that exceeds the size of the original, then
    // add a merge region larger than it. Anything outside the original snapshot
    // should be marked as changed.
    std::vector<uint8_t> dataExtra = { 2, 2, 2 };
    std::vector<uint8_t> expectedDataExtra = { 0, 0, 2, 2, 2, 0, 0 };
    int extraOffset = snapSize + HOST_PAGE_SIZE + 10;
    std::memcpy(
      sharedMem.get() + extraOffset, dataExtra.data(), dataExtra.size());

    int extraRegionOffset = extraOffset - 2;
    int extraRegionSize = dataExtra.size() + 4;
    snap->addMergeRegion(extraRegionOffset,
                         extraRegionSize,
                         SnapshotDataType::Raw,
                         SnapshotMergeOperation::Overwrite);

    // Include an offset which doesn't change the data.get(), but will register
    // a dirty page
    std::vector<uint8_t> dataNoChange = { 0, 0, 0 };
    int offsetNoChange = 4 * HOST_PAGE_SIZE - 10;
    std::memcpy(sharedMem.get() + offsetNoChange,
                dataNoChange.data(),
                dataNoChange.size());

    // Check shared memory does have dirty pages (including the non-change)
    std::vector<int> sharedDirtyPages =
      getDirtyPageNumbers(sharedMem.get(), sharedMemPages);
    std::vector<int> expected = { 1, 2, 3, 6 };
    REQUIRE(sharedDirtyPages == expected);

    // Check we have the right number of diffs
    std::vector<SnapshotDiff> changeDiffs =
      MemoryView({ sharedMem.get(), sharedMemSize }).diffWithSnapshot(snap);

    REQUIRE(changeDiffs.size() == 6);

    checkSnapshotDiff(offsetA, dataA, changeDiffs.at(0));
    checkSnapshotDiff(offsetB1, dataB1, changeDiffs.at(1));
    checkSnapshotDiff(offsetB2, dataB2, changeDiffs.at(2));
    checkSnapshotDiff(offsetC, expectedDataC, changeDiffs.at(3));
    checkSnapshotDiff(regionOffsetD, expectedDataD, changeDiffs.at(4));
    checkSnapshotDiff(extraRegionOffset, expectedDataExtra, changeDiffs.at(5));
}
}
