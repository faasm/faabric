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
    REQUIRE(offset == actual.offset);
    std::vector<uint8_t> actualData(actual.data, actual.data + actual.size);
    REQUIRE(data == actualData);
}

TEST_CASE_METHOD(SnapshotTestFixture, "Test snapshot diffs", "[snapshot]")
{
    std::string snapKey = "foobar123";
    int snapPages = 5;
    size_t snapSize = snapPages * HOST_PAGE_SIZE;
    SnapshotData snap = takeSnapshot(snapKey, snapPages, true);

    // Make shared memory larger than original snapshot
    int sharedMemPages = 8;
    size_t sharedMemSize = sharedMemPages * HOST_PAGE_SIZE;
    uint8_t* sharedMem = allocatePages(sharedMemPages);

    // Map the snapshot to the start of the memory
    reg.mapSnapshot(snapKey, sharedMem);

    // Reset dirty tracking
    faabric::util::resetDirtyTracking();

    // Set up some chunks of data to write into the memory
    std::vector<uint8_t> dataA = { 1, 2, 3, 4 };
    std::vector<uint8_t> dataB = { 4, 5, 6 };
    std::vector<uint8_t> dataC = { 7, 6, 5, 4, 3 };
    std::vector<uint8_t> dataD = { 1, 1, 1, 1 };

    // Set up some offsets, both on and over page boundaries
    int offsetA = HOST_PAGE_SIZE;
    int offsetB = HOST_PAGE_SIZE + 20;
    int offsetC = 2 * HOST_PAGE_SIZE - 2;
    int offsetD = 3 * HOST_PAGE_SIZE - dataD.size();

    // Write the data
    std::memcpy(sharedMem + offsetA, dataA.data(), dataA.size());
    std::memcpy(sharedMem + offsetB, dataB.data(), dataB.size());
    std::memcpy(sharedMem + offsetC, dataC.data(), dataC.size());
    std::memcpy(sharedMem + offsetD, dataD.data(), dataD.size());

    // Write the data to the region that exceeds the size of the original
    std::vector<uint8_t> dataExtra(
      (sharedMemPages - snapPages) * HOST_PAGE_SIZE, 5);
    std::memcpy(sharedMem + snapSize, dataExtra.data(), dataExtra.size());

    // Include an offset which doesn't change the data, but will register a
    // dirty page
    std::vector<uint8_t> dataNoChange = { 0, 0, 0 };
    int offsetNoChange = 4 * HOST_PAGE_SIZE - 10;
    std::memcpy(
      sharedMem + offsetNoChange, dataNoChange.data(), dataNoChange.size());

    // Check original has no dirty pages
    REQUIRE(snap.getDirtyPages().empty());

    // Check shared memory does have dirty pages (including the non-change)
    std::vector<int> sharedDirtyPages =
      getDirtyPageNumbers(sharedMem, sharedMemPages);
    std::vector<int> expected = { 1, 2, 3, 5, 6, 7 };
    REQUIRE(sharedDirtyPages == expected);

    // Check change diffs note that diffs across page boundaries will be split
    // into two
    std::vector<SnapshotDiff> changeDiffs =
      snap.getChangeDiffs(sharedMem, sharedMemSize);
    REQUIRE(changeDiffs.size() == 6);

    // One chunk will be split over 2 pages
    std::vector<uint8_t> dataCPart1 = { 7, 6 };
    std::vector<uint8_t> dataCPart2 = { 5, 4, 3 };
    int offsetC2 = 2 * HOST_PAGE_SIZE;

    checkSnapshotDiff(offsetA, dataA, changeDiffs.at(0));
    checkSnapshotDiff(offsetB, dataB, changeDiffs.at(1));
    checkSnapshotDiff(offsetC, dataCPart1, changeDiffs.at(2));
    checkSnapshotDiff(offsetC2, dataCPart2, changeDiffs.at(3));
    checkSnapshotDiff(offsetD, dataD, changeDiffs.at(4));
    checkSnapshotDiff(snapSize, dataExtra, changeDiffs.at(5));
}
}
