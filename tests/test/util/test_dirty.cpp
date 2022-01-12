#include <catch2/catch.hpp>

#include "fixtures.h"

#include <faabric/util/dirty.h>
#include <faabric/util/memory.h>

#include <cstring>
#include <sys/mman.h>
#include <unistd.h>

using namespace faabric::util;

namespace tests {

class DirtyConfTestFixture
  : public ConfTestFixture
  , public DirtyTrackingTestFixture
{
  public:
    DirtyConfTestFixture() = default;
    ~DirtyConfTestFixture() = default;
};

TEST_CASE_METHOD(DirtyConfTestFixture,
                 "Test dirty page checking",
                 "[util][dirty]")
{
    SECTION("Soft dirty PTEs") { conf.dirtyTrackingMode = "softpte"; }

    SECTION("Segfaults") { conf.dirtyTrackingMode = "segfault"; }

    DirtyTracker& tracker = getDirtyTracker();

    // Create several pages of memory
    int nPages = 6;
    size_t memSize = HOST_PAGE_SIZE * nPages;
    MemoryRegion memPtr = allocatePrivateMemory(memSize);
    std::span<uint8_t> memView(memPtr.get(), memSize);

    tracker.clearAll();

    std::vector<char> actual = tracker.getBothDirtyPages(memView);
    std::vector<char> expected(nPages, 0);
    REQUIRE(actual == expected);

    tracker.startTracking(memView);
    tracker.startThreadLocalTracking(memView);

    // Dirty two of the pages
    uint8_t* pageZero = memPtr.get();
    uint8_t* pageOne = pageZero + HOST_PAGE_SIZE;
    uint8_t* pageThree = pageOne + (2 * HOST_PAGE_SIZE);

    pageOne[10] = 1;
    pageThree[123] = 4;

    expected = { 0, 1, 0, 1, 0, 0 };

    actual = tracker.getBothDirtyPages(memView);
    REQUIRE(actual == expected);

    // And another
    uint8_t* pageFive = pageThree + (2 * HOST_PAGE_SIZE);
    pageFive[99] = 3;

    expected[5] = 1;
    actual = tracker.getBothDirtyPages(memView);
    REQUIRE(actual == expected);

    // Reset
    tracker.stopTracking(memView);
    tracker.stopThreadLocalTracking(memView);
    tracker.startTracking(memView);
    tracker.startThreadLocalTracking(memView);

    actual = tracker.getBothDirtyPages(memView);
    expected = std::vector<char>(nPages, 0);
    REQUIRE(actual == expected);

    // Check the data hasn't changed
    REQUIRE(pageOne[10] == 1);
    REQUIRE(pageThree[123] == 4);
    REQUIRE(pageFive[99] == 3);

    // Set some other data
    uint8_t* pageFour = pageThree + HOST_PAGE_SIZE;
    pageThree[100] = 2;
    pageFour[22] = 5;

    // As pages are adjacent we get a single region
    expected = std::vector<char>(nPages, 0);
    expected[3] = 1;
    expected[4] = 1;
    actual = tracker.getBothDirtyPages(memView);
    REQUIRE(actual == expected);

    // Final reset and check
    tracker.stopTracking(memView);
    tracker.stopThreadLocalTracking(memView);

    tracker.startTracking(memView);
    tracker.startThreadLocalTracking(memView);
    actual = tracker.getBothDirtyPages(memView);
    expected = std::vector<char>(nPages, 0);
    REQUIRE(actual == expected);

    tracker.stopTracking(memView);
    tracker.stopThreadLocalTracking(memView);
}

TEST_CASE_METHOD(DirtyConfTestFixture,
                 "Test dirty region checking",
                 "[util][dirty]")
{
    SECTION("Segfaults") { conf.dirtyTrackingMode = "segfault"; }

    SECTION("Soft PTEs") { conf.dirtyTrackingMode = "softpte"; }

    tracker = getDirtyTracker();

    int nPages = 15;
    size_t memSize = HOST_PAGE_SIZE * nPages;
    MemoryRegion mem = allocateSharedMemory(memSize);
    std::span<uint8_t> memView(mem.get(), memSize);

    DirtyTracker& tracker = getDirtyTracker();
    tracker.clearAll();

    std::vector<char> actual =
      tracker.getBothDirtyPages({ mem.get(), memSize });
    std::vector<char> expected(nPages, 0);
    REQUIRE(actual == expected);

    tracker.startTracking(memView);
    tracker.startThreadLocalTracking(memView);

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

    expected[0] = 1;
    expected[1] = 1;
    expected[3] = 1;
    expected[4] = 1;
    expected[7] = 1;
    expected[9] = 1;

    tracker.stopTracking({ mem.get(), memSize });
    tracker.stopThreadLocalTracking({ mem.get(), memSize });

    actual = tracker.getBothDirtyPages({ mem.get(), memSize });

    REQUIRE(actual.size() == expected.size());

    REQUIRE(actual == expected);
}

TEST_CASE_METHOD(DirtyConfTestFixture,
                 "Test segfault tracking",
                 "[util][dirty]")
{
    conf.dirtyTrackingMode = "segfault";
    tracker = getDirtyTracker();

    int nPages = 10;
    size_t memSize = nPages * HOST_PAGE_SIZE;
    std::vector<uint8_t> expectedData(memSize, 5);
    std::vector<char> expectedDirty(nPages, 0);

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
    tracker.startTracking(memView);
    tracker.startThreadLocalTracking(memView);

    // Make a change on one page
    size_t offsetA = 0;
    mem[offsetA] = 3;
    expectedData[offsetA] = 3;
    expectedDirty[0] = 1;

    // Make two changes on adjacent page
    size_t offsetB1 = HOST_PAGE_SIZE + 10;
    size_t offsetB2 = HOST_PAGE_SIZE + 50;
    mem[offsetB1] = 4;
    mem[offsetB2] = 2;
    expectedData[offsetB1] = 4;
    expectedData[offsetB2] = 2;
    expectedDirty[1] = 1;

    // Change another page
    size_t offsetC = (5 * HOST_PAGE_SIZE) + 10;
    mem[offsetC] = 6;
    expectedData[offsetC] = 6;
    expectedDirty[5] = 1;

    // Just read from another (should not cause a diff)
    int readValue = mem[4 * HOST_PAGE_SIZE + 5];
    REQUIRE(readValue == 5);

    // Check writes have propagated to the actual memory
    std::vector<uint8_t> actualMemAfter(mem.get(), mem.get() + memSize);
    REQUIRE(actualMemAfter == expectedData);

    // Get dirty regions
    std::vector<char> actualDirty = tracker.getBothDirtyPages(memView);

    // Check dirty regions
    REQUIRE(actualDirty == expectedDirty);

    tracker.stopTracking(memView);
    tracker.stopThreadLocalTracking(memView);
}

TEST_CASE_METHOD(DirtyConfTestFixture,
                 "Test multi-threaded segfault tracking",
                 "[util][dirty]")
{
    // Here we want to check that faults triggered in a given thread are caught
    // by that thread, and so we can safely just to thread-local diff tracking.
    conf.dirtyTrackingMode = "segfault";
    tracker = getDirtyTracker();

    int nLoops = 20;

    // Deliberately cause contention
    int nThreads = 100;
    int nPages = 2 * nThreads;
    size_t memSize = nPages * HOST_PAGE_SIZE;

    MemoryRegion mem = allocatePrivateMemory(memSize);
    std::span<uint8_t> memView(mem.get(), memSize);

    for (int loop = 0; loop < nLoops; loop++) {
        std::vector<std::shared_ptr<std::atomic<bool>>> success;
        success.resize(nThreads);

        // Start global tracking
        tracker.startTracking(memView);

        std::vector<std::thread> threads;
        threads.reserve(nThreads);
        for (int i = 0; i < nThreads; i++) {
            threads.emplace_back([this, &success, &memView, &nPages, i, loop] {
                success.at(i) = std::make_shared<std::atomic<bool>>();

                // Start thread-local tracking
                tracker.startThreadLocalTracking(memView);

                // Modify a couple of pages specific to this thread
                size_t pageOffset = i * 2;
                size_t byteOffset = pageOffset * HOST_PAGE_SIZE;
                uint8_t* pageOne = memView.data() + byteOffset;
                uint8_t* pageTwo = memView.data() + byteOffset + HOST_PAGE_SIZE;

                pageOne[20] = 3;
                pageOne[250] = 5;
                pageOne[HOST_PAGE_SIZE - 20] = 6;

                pageTwo[35] = 2;
                pageTwo[HOST_PAGE_SIZE - 100] = 3;

                tracker.stopThreadLocalTracking(memView);

                // Check we get the right number of dirty regions
                std::vector<char> regions =
                  tracker.getThreadLocalDirtyPages(memView);
                if (regions.size() != nPages) {
                    SPDLOG_ERROR("Segfault thread {} failed on loop {}. Got {} "
                                 "regions instead of {}",
                                 i,
                                 loop,
                                 regions.size(),
                                 1);
                    return;
                }

                std::vector<char> expected = std::vector<char>(nPages, 0);
                expected[pageOffset] = 1;
                expected[pageOffset + 1] = 1;

                if (regions != expected) {
                    SPDLOG_ERROR(
                      "Segfault thread {} failed on loop {}. Regions not equal",
                      i,
                      loop);
                    success.at(i)->store(false);
                } else {
                    success.at(i)->store(true);
                }
            });
        }

        for (auto& t : threads) {
            if (t.joinable()) {
                t.join();
            }
        }

        // Stop tracking
        tracker.stopTracking(memView);

        // Check no global offsets
        REQUIRE(tracker.getDirtyPages(memView).empty());

        bool thisLoopSuccess = true;
        for (int i = 0; i < nThreads; i++) {
            if (!success.at(i)->load()) {
                SPDLOG_ERROR(
                  "Segfault thread test thread {} on loop {} failed", i, loop);
                thisLoopSuccess = false;
            }
        }

        REQUIRE(thisLoopSuccess);
    }
}
}
