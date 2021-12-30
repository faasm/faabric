#include <catch2/catch.hpp>

#include "fixtures.h"

#include <faabric/util/dirty.h>
#include <faabric/util/macros.h>
#include <faabric/util/memory.h>

#include <cstring>
#include <sys/mman.h>
#include <unistd.h>

using namespace faabric::util;

namespace tests {

TEST_CASE("Test rounding down offsets to page size", "[memory]")
{
    REQUIRE(faabric::util::alignOffsetDown(2 * faabric::util::HOST_PAGE_SIZE) ==
            2 * faabric::util::HOST_PAGE_SIZE);
    REQUIRE(
      faabric::util::alignOffsetDown(2 * faabric::util::HOST_PAGE_SIZE + 25) ==
      2 * faabric::util::HOST_PAGE_SIZE);

    REQUIRE(faabric::util::alignOffsetDown(0) == 0);
    REQUIRE(faabric::util::alignOffsetDown(22) == 0);

    REQUIRE(
      faabric::util::alignOffsetDown(867 * faabric::util::HOST_PAGE_SIZE) ==
      867 * faabric::util::HOST_PAGE_SIZE);
    REQUIRE(
      faabric::util::alignOffsetDown(867 * faabric::util::HOST_PAGE_SIZE - 1) ==
      866 * faabric::util::HOST_PAGE_SIZE);
}

TEST_CASE("Check CoW memory mapping", "[memory]")
{
    size_t memSize = getpagesize();

    // Create a memory region with some data
    void* sharedVoid =
      mmap(nullptr, memSize, PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    int* sharedInt = reinterpret_cast<int*>(sharedVoid);
    for (int i = 0; i < 10; i++) {
        sharedInt[i] = i;
    }

    // Create an anonymous file, put in some data
    int fd = memfd_create("foobar", 0);
    ftruncate(fd, memSize);
    write(fd, sharedVoid, memSize);

    // Create two larger memory regions not yet writeable
    void* regionAVoid =
      mmap(nullptr, 3 * memSize, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    void* regionBVoid =
      mmap(nullptr, 3 * memSize, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    uint8_t* regionABytes = BYTES(regionAVoid);
    uint8_t* regionBBytes = BYTES(regionBVoid);

    // Allow writing to the middle segment of both
    mprotect(regionABytes + memSize, memSize, PROT_WRITE);
    mprotect(regionBBytes + memSize, memSize, PROT_WRITE);

    // Make one a CoW mapping onto the shared region
    mmap(regionABytes + memSize,
         memSize,
         PROT_WRITE,
         MAP_PRIVATE | MAP_FIXED,
         fd,
         0);

    // Get pointers now writable/ mapped
    int* regionAInt = reinterpret_cast<int*>(regionABytes + memSize);
    int* regionBInt = reinterpret_cast<int*>(regionBBytes + memSize);

    // Check contents
    for (int i = 0; i < 10; i++) {
        REQUIRE(sharedInt[i] == i);
        REQUIRE(regionAInt[i] == i);
        REQUIRE(regionBInt[i] == 0);
    }

    // Now write to CoW, should not be reflected in original
    for (int i = 0; i < 10; i++) {
        regionAInt[i] = 2 * i;
    }

    // Check contents
    for (int i = 0; i < 10; i++) {
        REQUIRE(sharedInt[i] == i);
        REQUIRE(regionAInt[i] == 2 * i);
        REQUIRE(regionBInt[i] == 0);
    }

    // Tidy up
    munmap(sharedVoid, memSize);
    munmap(regionAVoid, 3 * memSize);
    munmap(regionBVoid, 3 * memSize);
}

TEST_CASE("Check shared memory mapping", "[memory]")
{
    int pageSize = getpagesize();
    size_t memSize = 4 * pageSize;

    // Create a shared memory region
    void* sharedVoid =
      mmap(nullptr, memSize, PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);

    // Create two other memory regions
    void* regionAVoid =
      mmap(nullptr, memSize, PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    void* regionBVoid =
      mmap(nullptr, memSize, PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);

    // Sanity check a write to the first region
    int* sharedInt = reinterpret_cast<int*>(sharedVoid);
    int* regionAInt = reinterpret_cast<int*>(regionAVoid);
    int* regionBInt = reinterpret_cast<int*>(regionBVoid);

    sharedInt[0] = 11;
    sharedInt[1] = 22;

    REQUIRE(sharedInt[0] == 11);
    REQUIRE(sharedInt[1] == 22);
    REQUIRE(regionAInt[0] == 0);
    REQUIRE(regionAInt[1] == 0);
    REQUIRE(regionBInt[0] == 0);
    REQUIRE(regionBInt[1] == 0);

    // Map the shared region onto both of the other regions
    mremap(sharedVoid, 0, memSize, MREMAP_FIXED | MREMAP_MAYMOVE, regionAVoid);
    mremap(sharedVoid, 0, memSize, MREMAP_FIXED | MREMAP_MAYMOVE, regionBVoid);

    // Check changes reflected
    REQUIRE(sharedInt[0] == 11);
    REQUIRE(sharedInt[1] == 22);
    REQUIRE(regionAInt[0] == 11);
    REQUIRE(regionAInt[1] == 22);
    REQUIRE(regionBInt[0] == 11);
    REQUIRE(regionBInt[1] == 22);

    // Check update in mapped region propagates
    regionAInt[1] = 33;
    REQUIRE(sharedInt[0] == 11);
    REQUIRE(sharedInt[1] == 33);
    REQUIRE(regionAInt[0] == 11);
    REQUIRE(regionAInt[1] == 33);
    REQUIRE(regionBInt[0] == 11);
    REQUIRE(regionBInt[1] == 33);

    // Check update in original propagates
    sharedInt[0] = 44;
    REQUIRE(sharedInt[0] == 44);
    REQUIRE(sharedInt[1] == 33);
    REQUIRE(regionAInt[0] == 44);
    REQUIRE(regionAInt[1] == 33);
    REQUIRE(regionBInt[0] == 44);
    REQUIRE(regionBInt[1] == 33);

    // Unmap and remap one of the mapped regions
    munmap(regionAVoid, memSize);
    mmap(regionAVoid, memSize, PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);

    regionAInt[0] = 6;
    REQUIRE(sharedInt[0] == 44);
    REQUIRE(sharedInt[1] == 33);
    REQUIRE(regionAInt[0] == 6);
    REQUIRE(regionAInt[1] == 0);
    REQUIRE(regionBInt[0] == 44);
    REQUIRE(regionBInt[1] == 33);

    // Check updates still propagate between shared and remaining mapping
    sharedInt[0] = 55;
    regionBInt[1] = 66;
    REQUIRE(sharedInt[0] == 55);
    REQUIRE(sharedInt[1] == 66);
    REQUIRE(regionAInt[0] == 6);
    REQUIRE(regionAInt[1] == 0);
    REQUIRE(regionBInt[0] == 55);
    REQUIRE(regionBInt[1] == 66);

    munmap(sharedVoid, memSize);
    munmap(regionAVoid, memSize);
    munmap(regionBVoid, memSize);
}

TEST_CASE("Test small aligned memory chunk", "[util]")
{
    AlignedChunk actual = getPageAlignedChunk(0, 10);

    REQUIRE(actual.originalOffset == 0);
    REQUIRE(actual.originalLength == 10);
    REQUIRE(actual.nBytesOffset == 0);
    REQUIRE(actual.nBytesLength == faabric::util::HOST_PAGE_SIZE);
    REQUIRE(actual.nPagesOffset == 0);
    REQUIRE(actual.nPagesLength == 1);
    REQUIRE(actual.offsetRemainder == 0);
}

TEST_CASE("Test aligned memory chunks near page boundaries", "[util]")
{
    long originalOffset = 2 * faabric::util::HOST_PAGE_SIZE - 1;
    long originalLength = 3;

    AlignedChunk actual = getPageAlignedChunk(originalOffset, originalLength);
    REQUIRE(actual.originalOffset == originalOffset);
    REQUIRE(actual.originalLength == originalLength);
    REQUIRE(actual.nPagesOffset == 1);
    REQUIRE(actual.nPagesLength == 2);
    REQUIRE(actual.nBytesOffset == 1 * faabric::util::HOST_PAGE_SIZE);
    REQUIRE(actual.nBytesLength == 2 * faabric::util::HOST_PAGE_SIZE);
    REQUIRE(actual.offsetRemainder == faabric::util::HOST_PAGE_SIZE - 1);
}

TEST_CASE("Test large offset memory chunk", "[util]")
{
    long originalOffset = 2 * faabric::util::HOST_PAGE_SIZE + 33;
    long originalLength = 5 * faabric::util::HOST_PAGE_SIZE + 123;

    AlignedChunk actual = getPageAlignedChunk(originalOffset, originalLength);
    REQUIRE(actual.originalOffset == originalOffset);
    REQUIRE(actual.originalLength == originalLength);
    REQUIRE(actual.nPagesOffset == 2);
    REQUIRE(actual.nPagesLength == 6);
    REQUIRE(actual.nBytesOffset == 2 * faabric::util::HOST_PAGE_SIZE);
    REQUIRE(actual.nBytesLength == 6 * faabric::util::HOST_PAGE_SIZE);
    REQUIRE(actual.offsetRemainder == 33);
}

TEST_CASE("Test already aligned memory chunk", "[util]")
{
    long originalOffset = 10 * faabric::util::HOST_PAGE_SIZE;
    long originalLength = 5 * faabric::util::HOST_PAGE_SIZE;

    AlignedChunk actual = getPageAlignedChunk(originalOffset, originalLength);
    REQUIRE(actual.originalOffset == originalOffset);
    REQUIRE(actual.originalLength == originalLength);
    REQUIRE(actual.nPagesOffset == 10);
    REQUIRE(actual.nPagesLength == 5);
    REQUIRE(actual.nBytesOffset == 10 * faabric::util::HOST_PAGE_SIZE);
    REQUIRE(actual.nBytesLength == 5 * faabric::util::HOST_PAGE_SIZE);
    REQUIRE(actual.offsetRemainder == 0);
}

TEST_CASE_METHOD(ConfTestFixture, "Test dirty page checking", "[util]")
{
    SECTION("Soft dirty PTEs") { conf.dirtyTrackingMode = "softpte"; }

    // SECTION("Segfaults") { conf.dirtyTrackingMode = "sigseg"; }

    // Create several pages of memory
    int nPages = 6;
    size_t memSize = faabric::util::HOST_PAGE_SIZE * nPages;
    MemoryRegion memPtr = allocatePrivateMemory(memSize);
    std::span<uint8_t> memView(memPtr.get(), memSize);

    if (memPtr == nullptr) {
        FAIL("Could not provision memory");
    }

    DirtyPageTracker& tracker = getDirtyPageTracker();
    tracker.clearAll();

    std::vector<std::pair<uint32_t, uint32_t>> actual =
      tracker.getDirtyOffsets(memView);
    REQUIRE(actual.empty());

    tracker.startTracking(memView);

    // Dirty two of the pages
    uint8_t* pageZero = memPtr.get();
    uint8_t* pageOne = pageZero + faabric::util::HOST_PAGE_SIZE;
    uint8_t* pageThree = pageOne + (2 * faabric::util::HOST_PAGE_SIZE);

    pageOne[10] = 1;
    pageThree[123] = 4;

    std::vector<std::pair<uint32_t, uint32_t>> expected = {
        { HOST_PAGE_SIZE, 2 * HOST_PAGE_SIZE },
        { 3 * HOST_PAGE_SIZE, 4* HOST_PAGE_SIZE }
    };
    actual = tracker.getDirtyOffsets(memView);
    REQUIRE(actual == expected);

    // And another
    uint8_t* pageFive = pageThree + (2 * faabric::util::HOST_PAGE_SIZE);
    pageFive[99] = 3;

    expected.emplace_back(5 * HOST_PAGE_SIZE, 6 * HOST_PAGE_SIZE);
    actual = tracker.getDirtyOffsets(memView);
    REQUIRE(actual == expected);

    // Reset
    tracker.restartTracking(memView);

    actual = tracker.getDirtyOffsets(memView);
    REQUIRE(actual.empty());

    // Check the data hasn't changed
    REQUIRE(pageOne[10] == 1);
    REQUIRE(pageThree[123] == 4);
    REQUIRE(pageFive[99] == 3);

    // Set some other data
    uint8_t* pageFour = pageThree + faabric::util::HOST_PAGE_SIZE;
    pageThree[100] = 2;
    pageFour[22] = 5;

    // As pages are adjacent we get a single region
    expected = {
        { 3 * HOST_PAGE_SIZE, 5 * HOST_PAGE_SIZE },
    };
    actual = tracker.getDirtyOffsets(memView);
    REQUIRE(actual == expected);

    // Final reset and check
    tracker.stopTracking(memView);
    actual = tracker.getDirtyOffsets(memView);
    REQUIRE(actual.empty());
}

TEST_CASE("Test dirty region checking", "[util]")
{
    int nPages = 15;
    size_t memSize = HOST_PAGE_SIZE * nPages;
    auto* sharedMemory = (uint8_t*)mmap(
      nullptr, memSize, PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);

    if (sharedMemory == nullptr) {
        FAIL("Could not provision memory");
    }

    faabric::util::DirtyPageTracker& tracker =
      faabric::util::getDirtyPageTracker();
    tracker.clearAll();

    std::vector<std::pair<uint32_t, uint32_t>> actual =
      tracker.getDirtyOffsets({ sharedMemory, memSize });
    REQUIRE(actual.empty());

    tracker.startTracking({ sharedMemory, memSize });

    // Dirty some pages, some adjacent
    uint8_t* pageZero = sharedMemory;
    uint8_t* pageOne = pageZero + HOST_PAGE_SIZE;
    uint8_t* pageThree = pageZero + (3 * HOST_PAGE_SIZE);
    uint8_t* pageFour = pageZero + (4 * HOST_PAGE_SIZE);
    uint8_t* pageSeven = pageZero + (7 * HOST_PAGE_SIZE);
    uint8_t* pageNine = pageZero + (9 * HOST_PAGE_SIZE);

    // Set some byte within each page
    pageZero[1] = 1;
    pageOne[11] = 1;
    pageThree[33] = 1;
    pageFour[44] = 1;
    pageSeven[77] = 1;
    pageNine[99] = 1;

    // Expect adjacent regions to be merged
    std::vector<std::pair<uint32_t, uint32_t>> expected = {
        { 0, 2 * HOST_PAGE_SIZE },
        { 3 * HOST_PAGE_SIZE, 5 * HOST_PAGE_SIZE },
        { 7 * HOST_PAGE_SIZE, 8 * HOST_PAGE_SIZE },
        { 9 * HOST_PAGE_SIZE, 10 * HOST_PAGE_SIZE },
    };

    actual = tracker.getDirtyOffsets({ sharedMemory, memSize });
    REQUIRE(actual.size() == expected.size());
    for (int i = 0; i < actual.size(); i++) {
        REQUIRE(actual.at(i).first == expected.at(i).first);
        REQUIRE(actual.at(i).second == expected.at(i).second);
    }
}

TEST_CASE("Test allocating and claiming memory", "[util]")
{
    // Allocate some virtual memory
    size_t vMemSize = 100 * HOST_PAGE_SIZE;
    MemoryRegion vMem = allocateVirtualMemory(vMemSize);

    size_t sizeA = 10 * HOST_PAGE_SIZE;
    claimVirtualMemory({ vMem.get(), sizeA });

    // Write something to the new memory
    vMem[10] = 1;
    vMem[5 * HOST_PAGE_SIZE] = 2;
    vMem[6 * HOST_PAGE_SIZE + 10] = 3;

    size_t sizeB = 5 * HOST_PAGE_SIZE;
    claimVirtualMemory({ vMem.get() + sizeA, sizeB });

    // Write something to the new memory
    vMem[sizeA + 10] = 4;
    vMem[sizeA + 3 * HOST_PAGE_SIZE] = 5;
    vMem[sizeA + 4 * HOST_PAGE_SIZE + 10] = 6;

    // Check all edits still there
    REQUIRE(vMem[10] == 1);
    REQUIRE(vMem[5 * HOST_PAGE_SIZE] == 2);
    REQUIRE(vMem[6 * HOST_PAGE_SIZE + 10] == 3);
    REQUIRE(vMem[sizeA + 10] == 4);
    REQUIRE(vMem[sizeA + 3 * HOST_PAGE_SIZE] == 5);
    REQUIRE(vMem[sizeA + 4 * HOST_PAGE_SIZE + 10] == 6);
}

TEST_CASE("Test mapping memory", "[util]")
{
    size_t vMemSize = 100 * HOST_PAGE_SIZE;
    MemoryRegion vMem = allocateVirtualMemory(vMemSize);

    // Set up some data in memory
    std::vector<uint8_t> chunk(10 * HOST_PAGE_SIZE, 3);
    claimVirtualMemory({ vMem.get(), chunk.size() });
    std::memcpy(vMem.get(), chunk.data(), chunk.size());

    // Write this to a file descriptor
    int fd = createFd(chunk.size(), "foobar");
    writeToFd(fd, 0, { vMem.get(), chunk.size() });

    // Map some new memory to this fd
    MemoryRegion memA = allocatePrivateMemory(chunk.size());
    mapMemoryPrivate({ memA.get(), chunk.size() }, fd);

    std::vector<uint8_t> memAData(memA.get(), memA.get() + chunk.size());
    REQUIRE(memAData == chunk);

    // Extend the memory and copy some new data in
    std::vector<uint8_t> chunkB(5 * HOST_PAGE_SIZE, 4);
    claimVirtualMemory({ vMem.get() + chunk.size(), chunkB.size() });
    std::memcpy(vMem.get() + chunk.size(), chunkB.data(), chunkB.size());

    // Append the data to the fd
    appendDataToFd(fd, { chunkB.data(), chunkB.size() });

    // Map a region to both chunks
    MemoryRegion memB = allocatePrivateMemory(chunk.size() + chunkB.size());
    mapMemoryPrivate({ memB.get(), chunk.size() + chunkB.size() }, fd);

    // Check region now contains both bits of data
    std::vector<uint8_t> memBData(memB.get(),
                                  memB.get() + chunk.size() + chunkB.size());
    std::vector<uint8_t> expected;
    expected.insert(expected.end(), chunk.begin(), chunk.end());
    expected.insert(expected.end(), chunkB.begin(), chunkB.end());

    REQUIRE(memBData == expected);
}

TEST_CASE("Test mapping memory fails with invalid fd", "[util]")
{
    size_t memSize = 10 * HOST_PAGE_SIZE;
    MemoryRegion sharedMem = allocatePrivateMemory(memSize);

    int fd = 0;
    SECTION("Zero fd") { fd = 0; }

    SECTION("Negative fd") { fd = -2; }

    REQUIRE_THROWS(mapMemoryPrivate({ sharedMem.get(), memSize }, fd));
}

TEST_CASE("Test remapping memory", "[util]")
{
    // Set up some data
    size_t dataSize = 10 * HOST_PAGE_SIZE;
    std::vector<uint8_t> expectedData(dataSize, 3);

    // Write this to a file descriptor
    int fd = createFd(expectedData.size(), "foobar");
    writeToFd(fd, 0, { expectedData.data(), expectedData.size() });

    // Map some new memory to this fd
    MemoryRegion mappedMem = allocatePrivateMemory(dataSize);
    mapMemoryPrivate({ mappedMem.get(), dataSize }, fd);

    std::vector<uint8_t> actualData(mappedMem.get(),
                                    mappedMem.get() + dataSize);
    REQUIRE(actualData == expectedData);

    // Modify the memory
    std::vector<uint8_t> update(100, 4);
    size_t updateOffset = HOST_PAGE_SIZE + 10;
    std::memcpy(mappedMem.get() + updateOffset, update.data(), update.size());

    // Spot check to make sure update has been made
    REQUIRE(*(mappedMem.get() + (updateOffset + 5)) == (uint8_t)4);

    // Remap
    mapMemoryPrivate({ mappedMem.get(), dataSize }, fd);

    // Spot check to make sure update has been removed
    REQUIRE(*(mappedMem.get() + (updateOffset + 5)) == (uint8_t)3);

    // Check all data
    std::vector<uint8_t> actualDataAfter(mappedMem.get(),
                                         mappedMem.get() + dataSize);
    REQUIRE(actualDataAfter == expectedData);
}

TEST_CASE_METHOD(ConfTestFixture, "Test mprotect tracking", "[.]")
{
    conf.dirtyTrackingMode = "sigseg";

    size_t memSize = 10 * HOST_PAGE_SIZE;
    std::vector<uint8_t> expectedData(memSize, 5);

    MprotectRegionTracker t;
    MemoryRegion mem = allocatePrivateMemory(memSize);

    std::span<uint8_t> memView(mem.get(), memSize);

    SECTION("Mapped from fd")
    {
        // Create a file descriptor holding expected data
        int fd = createFd(memSize, "foobar");
        writeToFd(fd, 0, expectedData);

        // Map the memory
        mapMemoryPrivate(memView, fd);
    }

    SECTION("Standard alloc")
    {
        // Copy expected data into memory
        std::memcpy(mem.get(), expectedData.data(), memSize);
    }

    // Start tracking
    t.startTracking(memView);

    // Make a change on one page
    size_t offsetA = 0;
    mem[offsetA] = 3;
    expectedData[offsetA] = 3;

    // Make two changes on same page
    size_t offsetB1 = HOST_PAGE_SIZE + 10;
    size_t offsetB2 = HOST_PAGE_SIZE + 50;
    mem[offsetB1] = 4;
    mem[offsetB2] = 5;
    expectedData[offsetB1] = 4;
    expectedData[offsetB2] = 5;

    // Change another page
    size_t offsetC = (5 * HOST_PAGE_SIZE) + 10;
    mem[offsetC] = 6;
    expectedData[offsetC] = 6;

    // Check writes have propagated to the actual memory
    std::vector<uint8_t> actualMemAfter(mem.get(), mem.get() + memSize);
    REQUIRE(actualMemAfter == expectedData);

    // Get dirty regions
    std::vector<std::pair<uint32_t, uint32_t>> actualDirty =
      t.getDirtyOffsets(memView);

    // Check dirty regions
    REQUIRE(actualDirty.size() == 3);

    std::vector<std::pair<uint32_t, uint32_t>> expectedDirty = {
        { 0, HOST_PAGE_SIZE },
        { HOST_PAGE_SIZE, 2 * HOST_PAGE_SIZE },
        { 5 * HOST_PAGE_SIZE, 6 * HOST_PAGE_SIZE }
    };

    REQUIRE(actualDirty == expectedDirty);

    t.stop();
}

}
