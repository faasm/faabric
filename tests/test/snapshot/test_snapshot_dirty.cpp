#include <catch.hpp>

#include "faabric/util/snapshot.h"
#include "faabric_utils.h"

#include <sys/mman.h>

#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/util/memory.h>

using namespace faabric::snapshot;

namespace tests {

static uint8_t* allocatePages(int nPages)
{
    return (uint8_t*)mmap(nullptr,
                          nPages * faabric::util::HOST_PAGE_SIZE,
                          PROT_WRITE,
                          MAP_SHARED | MAP_ANONYMOUS,
                          -1,
                          0);
}

static void deallocatePages(uint8_t* base, int nPages)
{
    munmap(base, nPages * faabric::util::HOST_PAGE_SIZE);
}

TEST_CASE_METHOD(SnapshotTestFixture,
                 "Test snapshot dirty page checks",
                 "[snapshot]")
{

    std::string snapKey = "dirty-check-snap";

    int nPages = 100;

    faabric::util::SnapshotData snap;
    snap.data = allocatePages(100);
    snap.size = nPages * faabric::util::HOST_PAGE_SIZE;
    reg.takeSnapshot(snapKey, snap, true);

    uint8_t* sharedMem = allocatePages(nPages);

    // Reset tracking before mapping
    faabric::util::resetDirtyTracking();

    // Map the memory
    reg.mapSnapshot(snapKey, sharedMem);

    // Mapping will cause dirty pages on only the mapped region
    std::vector<faabric::util::SnapshotDiff> snapDiffs = snap.getDirtyPages();
    REQUIRE(snapDiffs.empty());

    std::vector<bool> sharedMemDiffs =
      faabric::util::getDirtyPages(sharedMem, nPages);
    REQUIRE(sharedMemDiffs.size() == nPages);

    // Reset standard dirty page check and check nothing changes
    faabric::util::resetDirtyTracking();
    REQUIRE(snap.getDirtyPages().empty());
    REQUIRE(faabric::util::getDirtyPages(sharedMem, nPages).size() == nPages);

    // Check that the mapped region is "clean"
    REQUIRE(
      faabric::util::getDirtyPagesForMappedMemory(sharedMem, nPages).empty());

    // Write to shared mem, check only shared mem is dirty
    sharedMem[2 * faabric::util::HOST_PAGE_SIZE + 2] = 1;
    sharedMem[5 * faabric::util::HOST_PAGE_SIZE + 2] = 1;
    sharedMem[20 * faabric::util::HOST_PAGE_SIZE + 2] = 1;

    REQUIRE(snap.getDirtyPages().empty());

    REQUIRE(
      faabric::util::getDirtyPagesForMappedMemory(sharedMem, nPages).size() ==
      3);

    // Tidy up
    deallocatePages(sharedMem, nPages);
}
}
