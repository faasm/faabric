#include <catch2/catch.hpp>

#include "fixtures.h"

#include <faabric/util/dirty.h>
#include <faabric/util/memory.h>

#include <cstring>
#include <sys/mman.h>
#include <unistd.h>

using namespace faabric::util;

namespace tests {

TEST_CASE_METHOD(ConfTestFixture, "Test dirty page checking", "[util][dirty]")
{
    SECTION("Soft dirty PTEs") { conf.dirtyTrackingMode = "softpte"; }

    SECTION("Segfaults") { conf.dirtyTrackingMode = "segfault"; }

    DirtyPageTracker& tracker = getDirtyPageTracker();

    // Create several pages of memory
    int nPages = 6;
    size_t memSize = faabric::util::HOST_PAGE_SIZE * nPages;
    MemoryRegion memPtr = allocatePrivateMemory(memSize);
    std::span<uint8_t> memView(memPtr.get(), memSize);

    tracker.clearAll();

    std::vector<OffsetMemoryRegion> actual = tracker.getDirtyOffsets(memView);
    REQUIRE(actual.empty());

    tracker.startTracking(memView);

    // Dirty two of the pages
    uint8_t* pageZero = memPtr.get();
    uint8_t* pageOne = pageZero + faabric::util::HOST_PAGE_SIZE;
    uint8_t* pageThree = pageOne + (2 * faabric::util::HOST_PAGE_SIZE);

    pageOne[10] = 1;
    pageThree[123] = 4;

    std::vector<OffsetMemoryRegion> expected = {
        OffsetMemoryRegion(
          HOST_PAGE_SIZE,
          std::span<uint8_t>(memPtr.get() + HOST_PAGE_SIZE, HOST_PAGE_SIZE)),
        OffsetMemoryRegion(
          3 * HOST_PAGE_SIZE,
          std::span<uint8_t>(memPtr.get() + 3 * HOST_PAGE_SIZE, HOST_PAGE_SIZE))
    };

    actual = tracker.getDirtyOffsets(memView);
    REQUIRE(actual == expected);

    // And another
    uint8_t* pageFive = pageThree + (2 * faabric::util::HOST_PAGE_SIZE);
    pageFive[99] = 3;

    expected.emplace_back(
      5 * HOST_PAGE_SIZE,
      std::span<uint8_t>(memView.data() + 5 * HOST_PAGE_SIZE, HOST_PAGE_SIZE));
    actual = tracker.getDirtyOffsets(memView);
    REQUIRE(actual == expected);

    // Reset
    tracker.stopTracking(memView);
    tracker.startTracking(memView);

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
        OffsetMemoryRegion(3 * HOST_PAGE_SIZE,
                           std::span<uint8_t>(memPtr.get() + 3 * HOST_PAGE_SIZE,
                                              2 * HOST_PAGE_SIZE)),
    };
    actual = tracker.getDirtyOffsets(memView);
    REQUIRE(actual == expected);

    // Final reset and check
    tracker.stopTracking(memView);

    tracker.startTracking(memView);
    actual = tracker.getDirtyOffsets(memView);
    REQUIRE(actual.empty());

    tracker.stopTracking(memView);
}

TEST_CASE_METHOD(ConfTestFixture, "Test dirty region checking", "[util][dirty]")
{
    SECTION("Segfaults") { conf.dirtyTrackingMode = "segfault"; }

    SECTION("Soft PTEs") { conf.dirtyTrackingMode = "softpte"; }

    int nPages = 15;
    size_t memSize = HOST_PAGE_SIZE * nPages;
    MemoryRegion mem = allocateSharedMemory(memSize);

    faabric::util::DirtyPageTracker& tracker =
      faabric::util::getDirtyPageTracker();
    tracker.clearAll();

    std::vector<OffsetMemoryRegion> actual =
      tracker.getDirtyOffsets({ mem.get(), memSize });
    REQUIRE(actual.empty());

    tracker.startTracking({ mem.get(), memSize });

    // Dirty some pages, some adjacent
    uint8_t* pageZero = mem.get();
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

    tracker.stopTracking({ mem.get(), memSize });

    // Expect adjacent regions to be merged
    std::vector<OffsetMemoryRegion> expected = {
        OffsetMemoryRegion(0,
                           std::span<uint8_t>(mem.get(), 2 * HOST_PAGE_SIZE)),
        OffsetMemoryRegion(3 * HOST_PAGE_SIZE,
                           std::span<uint8_t>(mem.get() + 3 * HOST_PAGE_SIZE,
                                              2 * HOST_PAGE_SIZE)),
        OffsetMemoryRegion(
          7 * HOST_PAGE_SIZE,
          std::span<uint8_t>(mem.get() + 7 * HOST_PAGE_SIZE, HOST_PAGE_SIZE)),
        OffsetMemoryRegion(
          9 * HOST_PAGE_SIZE,
          std::span<uint8_t>(mem.get() + 9 * HOST_PAGE_SIZE, HOST_PAGE_SIZE))
    };

    actual = tracker.getDirtyOffsets({ mem.get(), memSize });

    REQUIRE(actual.size() == expected.size());

    REQUIRE(actual == expected);
}

TEST_CASE_METHOD(ConfTestFixture, "Test segfault tracking", "[util][dirty]")
{
    conf.dirtyTrackingMode = "sigseg";

    size_t memSize = 10 * HOST_PAGE_SIZE;
    std::vector<uint8_t> expectedData(memSize, 5);

    SegfaultDirtyTracker t;
    MemoryRegion mem = allocatePrivateMemory(memSize);

    std::span<uint8_t> memView(mem.get(), memSize);

    SECTION("Standard alloc")
    {
        // Copy expected data into memory
        std::memcpy(mem.get(), expectedData.data(), memSize);
    }

    SECTION("Mapped from fd")
    {
        // Create a file descriptor holding expected data
        int fd = createFd(memSize, "foobar");
        writeToFd(fd, 0, expectedData);

        // Map the memory
        mapMemoryPrivate(memView, fd);
    }

    // Check memory to start with
    std::vector<uint8_t> actualMemBefore(mem.get(), mem.get() + memSize);
    REQUIRE(actualMemBefore == expectedData);

    // Start tracking
    t.startTracking(memView);

    // Make a change on one page
    size_t offsetA = 0;
    mem[offsetA] = 3;
    expectedData[offsetA] = 3;

    // Make two changes on adjacent page
    size_t offsetB1 = HOST_PAGE_SIZE + 10;
    size_t offsetB2 = HOST_PAGE_SIZE + 50;
    mem[offsetB1] = 4;
    mem[offsetB2] = 2;
    expectedData[offsetB1] = 4;
    expectedData[offsetB2] = 2;

    // Change another page
    size_t offsetC = (5 * HOST_PAGE_SIZE) + 10;
    mem[offsetC] = 6;
    expectedData[offsetC] = 6;

    // Just read from another (should not cause a diff)
    int readValue = mem[4 * HOST_PAGE_SIZE + 5];
    REQUIRE(readValue == 5);

    // Check writes have propagated to the actual memory
    std::vector<uint8_t> actualMemAfter(mem.get(), mem.get() + memSize);
    REQUIRE(actualMemAfter == expectedData);

    // Get dirty regions
    std::vector<OffsetMemoryRegion> actualDirty = t.getDirtyOffsets(memView);

    // Check dirty regions
    REQUIRE(actualDirty.size() == 2);

    std::vector<OffsetMemoryRegion> expectedDirty = {
        { 0, memView.subspan(0, 2 * HOST_PAGE_SIZE) },
        { (uint32_t)(5 * HOST_PAGE_SIZE),
          memView.subspan(5 * HOST_PAGE_SIZE, HOST_PAGE_SIZE) }
    };

    REQUIRE(actualDirty == expectedDirty);

    t.stopTracking(memView);
}

TEST_CASE_METHOD(ConfTestFixture,
                 "Stress test segfault tracking",
                 "[util][dirty]")
{}
}
