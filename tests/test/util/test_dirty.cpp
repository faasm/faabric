#include <catch2/catch.hpp>

#include "fixtures.h"

#include <faabric/util/dirty.h>
#include <faabric/util/memory.h>

#include <cstring>
#include <sys/mman.h>
#include <unistd.h>

using namespace faabric::util;

namespace tests {

TEST_CASE_METHOD(DirtyTrackingTestFixture,
                 "Test configuring tracker",
                 "[util][dirty]")
{
    std::string mode;

    SECTION("Segfaults") { mode = "segfault"; }

    SECTION("Soft PTEs") { mode = "softpte"; }

    SECTION("None") { mode = "none"; }

    SECTION("Uffd") { mode = "uffd"; }

    SECTION("Uffd write-protect") { mode = "uffd-wp"; }

    SECTION("Uffd threaded") { mode = "uffd-thread"; }

    SECTION("Uffd threaded write-protect") { mode = "uffd-thread-wp"; }

    // Set the conf, reset the tracker and check
    setTrackingMode(mode);
    auto t = getDirtyTracker();
    REQUIRE(t->getType() == mode);
}

TEST_CASE_METHOD(DirtyTrackingTestFixture,
                 "Test basic dirty tracking",
                 "[util][dirty]")
{
    // Some dirty trackers are expected to work after being reset, others not
    bool checkPostReset = false;

    // Shared vs. private memory is an important distinction as userfaultfd may
    // perform differently
    bool sharedMemory = false;
    bool mappedMemory = false;

    // Some dirty trackers count reads as well as writes when marking pages as
    // dirty
    bool dirtyReads = false;

    SECTION("Soft dirty PTEs")
    {
        setTrackingMode("softpte");
        checkPostReset = true;
        dirtyReads = false;

        SECTION("Shared") { sharedMemory = true; }

        SECTION("Private") { sharedMemory = false; }

        SECTION("Mapped shared")
        {
            sharedMemory = true;
            mappedMemory = true;
        }

        SECTION("Mapped private")
        {
            sharedMemory = false;
            mappedMemory = true;
        }
    }

    SECTION("Segfaults")
    {
        setTrackingMode("segfault");
        checkPostReset = true;
        dirtyReads = false;

        SECTION("Shared") { sharedMemory = true; }

        SECTION("Private") { sharedMemory = false; }

        SECTION("Mapped shared")
        {
            sharedMemory = true;
            mappedMemory = true;
        }

        SECTION("Mapped private")
        {
            sharedMemory = false;
            mappedMemory = true;
        }
    }

    SECTION("Userfaultfd")
    {
        setTrackingMode("uffd");
        checkPostReset = false;
        dirtyReads = true;

        SECTION("Shared") { sharedMemory = true; }

        SECTION("Private") { sharedMemory = false; }

        SECTION("Mapped shared")
        {
            sharedMemory = true;
            mappedMemory = true;
        }

        SECTION("Mapped private")
        {
            sharedMemory = false;
            mappedMemory = true;
        }
    }

    SECTION("Userfaultfd wp")
    {
        setTrackingMode("uffd-wp");
        checkPostReset = true;
        dirtyReads = true;

        // Neither shared nor mapped mem works with write-protection

        SECTION("Private") { sharedMemory = false; }
    }

    SECTION("Userfaultfd thread")
    {
        setTrackingMode("uffd-thread");
        checkPostReset = false;
        dirtyReads = false;

        SECTION("Shared") { sharedMemory = true; }

        SECTION("Private") { sharedMemory = false; }

        SECTION("Mapped shared")
        {
            sharedMemory = true;
            mappedMemory = true;
        }

        SECTION("Mapped private")
        {
            sharedMemory = false;
            mappedMemory = true;
        }
    }

    SECTION("Userfaultfd thread wp")
    {
        setTrackingMode("uffd-thread-wp");
        checkPostReset = true;
        dirtyReads = false;

        // Neither shared nor mapped mem works with write-protection

        SECTION("Private") { sharedMemory = false; }
    }

    // Create several pages of memory
    int nPages = 6;
    size_t memSize = HOST_PAGE_SIZE * nPages;

    // Need to allocate both regions here otherwise destructors will nuke them
    // before we're done
    std::span<uint8_t> memView;
    MemoryRegion sharedMemPtr = allocateSharedMemory(memSize);
    MemoryRegion privateMemPtr = allocatePrivateMemory(memSize);
    if (sharedMemory) {
        memView = std::span<uint8_t>(sharedMemPtr.get(), memSize);
    } else {
        memView = std::span<uint8_t>(privateMemPtr.get(), memSize);
    }

    if (mappedMemory) {
        int fd = createFd(memSize, "foobar");

        // Map the memory
        if (sharedMemory) {
            mapMemoryShared(memView, fd);
        } else {
            mapMemoryPrivate(memView, fd);
        }
    }

    std::shared_ptr<DirtyTracker> tracker = getDirtyTracker();
    REQUIRE(tracker->getType() == conf.dirtyTrackingMode);

    // Make sure we clear all, relevant for anything with system-wide state
    tracker->clearAll();

    std::vector<char> actual = tracker->getBothDirtyPages(memView);
    std::vector<char> expected(nPages, 0);
    REQUIRE(actual == expected);

    tracker->startTracking(memView);
    tracker->startThreadLocalTracking(memView);

    // Get pointers to pages
    uint8_t* pageZero = memView.data();
    uint8_t* pageOne = pageZero + HOST_PAGE_SIZE;
    uint8_t* pageThree = pageOne + (2 * HOST_PAGE_SIZE);

    // Do a read
    int readValue = pageZero[1];
    REQUIRE(readValue == 0);

    // Do a couple of writes
    pageOne[10] = 1;
    pageThree[123] = 4;

    if (dirtyReads) {
        expected = { 1, 1, 0, 1, 0, 0 };
    } else {
        expected = { 0, 1, 0, 1, 0, 0 };
    }

    actual = tracker->getBothDirtyPages(memView);
    REQUIRE(actual == expected);

    // And another
    uint8_t* pageFive = pageThree + (2 * HOST_PAGE_SIZE);
    pageFive[99] = 3;

    expected[5] = 1;
    actual = tracker->getBothDirtyPages(memView);
    REQUIRE(actual == expected);

    // Reset
    tracker->stopTracking(memView);
    tracker->stopThreadLocalTracking(memView);
    tracker->startTracking(memView);
    tracker->startThreadLocalTracking(memView);

    actual = tracker->getBothDirtyPages(memView);
    expected = std::vector<char>(nPages, 0);
    REQUIRE(actual == expected);

    // Check the data hasn't changed
    REQUIRE(pageOne[10] == 1);
    REQUIRE(pageThree[123] == 4);
    REQUIRE(pageFive[99] == 3);

    if (checkPostReset) {
        // Set some other data, make sure we write to one of the pages already
        // modified in the first changes
        uint8_t* pageFour = pageThree + HOST_PAGE_SIZE;
        pageThree[100] = 2;
        pageFour[22] = 5;

        // Check dirty pages
        expected = std::vector<char>(nPages, 0);
        expected[3] = 1;
        expected[4] = 1;
        actual = tracker->getBothDirtyPages(memView);
        REQUIRE(actual == expected);

        // Final reset and check
        tracker->stopTracking(memView);
        tracker->stopThreadLocalTracking(memView);

        tracker->startTracking(memView);
        tracker->startThreadLocalTracking(memView);
        actual = tracker->getBothDirtyPages(memView);
        expected = std::vector<char>(nPages, 0);
        REQUIRE(actual == expected);

        tracker->stopTracking(memView);
        tracker->stopThreadLocalTracking(memView);
    }
}

TEST_CASE_METHOD(DirtyTrackingTestFixture,
                 "Test thread-local dirty tracking",
                 "[util][dirty]")
{
    // Here we just want to check that thread-local tracking works for the
    // trackers that support it, i.e. those based on signal handling

    // Certain trackers support repeat tracking on the same address space, while
    // others don't, so we may or may not loop
    int nLoops = 0;

    SECTION("Segfault")
    {
        setTrackingMode("segfault");
        nLoops = 20;
    }

    SECTION("Userfaultfd")
    {
        setTrackingMode("uffd");
        nLoops = 1;
    }

    SECTION("Userfaultfd wp")
    {
        setTrackingMode("uffd-wp");
        nLoops = 20;
    }

    std::shared_ptr<DirtyTracker> tracker = getDirtyTracker();
    REQUIRE(tracker->getType() == conf.dirtyTrackingMode);

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
        tracker->startTracking(memView);

        std::vector<std::thread> threads;
        threads.reserve(nThreads);
        for (int i = 0; i < nThreads; i++) {
            threads.emplace_back(
              [&tracker, &success, &memView, &nPages, i, loop] {
                  success.at(i) = std::make_shared<std::atomic<bool>>();

                  // Start thread-local tracking
                  tracker->startThreadLocalTracking(memView);

                  // Modify a couple of pages specific to this thread
                  size_t pageOffset = i * 2;
                  size_t byteOffset = pageOffset * HOST_PAGE_SIZE;
                  uint8_t* pageOne = memView.data() + byteOffset;
                  uint8_t* pageTwo =
                    memView.data() + byteOffset + HOST_PAGE_SIZE;

                  pageOne[20] = 3;
                  pageOne[250] = 5;
                  pageOne[HOST_PAGE_SIZE - 20] = 6;

                  pageTwo[35] = 2;
                  pageTwo[HOST_PAGE_SIZE - 100] = 3;

                  tracker->stopThreadLocalTracking(memView);

                  // Check we get the right size for the dirty pages
                  std::vector<char> dirtyPages =
                    tracker->getThreadLocalDirtyPages(memView);
                  if (dirtyPages.size() != nPages) {
                      SPDLOG_ERROR("Thread {} failed on loop {}. Got {} "
                                   "regions instead of {}",
                                   i,
                                   loop,
                                   dirtyPages.size(),
                                   1);
                      return;
                  }

                  std::vector<char> expected = std::vector<char>(nPages, 0);
                  expected[pageOffset] = 1;
                  expected[pageOffset + 1] = 1;

                  if (dirtyPages != expected) {
                      int actualCount =
                        std::count(dirtyPages.begin(), dirtyPages.end(), 1);
                      int expectedCount =
                        std::count(expected.begin(), expected.end(), 1);

                      SPDLOG_ERROR("Thread {} failed on loop {}. Regions not "
                                   "equal ({} != {})",
                                   i,
                                   loop,
                                   actualCount,
                                   expectedCount);

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
        tracker->stopTracking(memView);

        // Check no global offsets
        REQUIRE(tracker->getDirtyPages(memView).empty());

        bool thisLoopSuccess = true;
        for (int i = 0; i < nThreads; i++) {
            if (!success.at(i)->load()) {
                SPDLOG_ERROR(
                  "Signal test thread {} on loop {} failed", i, loop);
                thisLoopSuccess = false;
            }
        }

        REQUIRE(thisLoopSuccess);
        SPDLOG_DEBUG("Thread-local tracking loop {} succeeded", loop);
    }
}
}
