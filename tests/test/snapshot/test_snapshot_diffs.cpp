#include <catch.hpp>

#include "faabric_utils.h"

#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/util/memory.h>

using namespace faabric::snapshot;
using namespace faabric::util;

namespace tests {

TEST_CASE_METHOD(SnapshotTestFixture, "Test snapshot dirty pages", "[snapshot]")
{
    std::string snapKey = "foobar123";
    int nPages = 5;
    SnapshotData snap = takeSnapshot(snapKey, nPages, true);

    uint8_t* sharedMem = allocatePages(nPages);

    reg.mapSnapshot(snapKey, sharedMem);

    // Reset dirty tracking
    faabric::util::resetDirtyTracking();

    // Make a change to the shared memory
    *(sharedMem + 2 * HOST_PAGE_SIZE) = 2;

    // Check original has no dirty pages
    REQUIRE(snap.getDirtyPages().empty());

    // Check shared memory does have dirty pages
    std::vector<int> sharedDirtyPages = getDirtyPageNumbers(sharedMem, nPages);
    std::vector<int> expected = { 2 };
    REQUIRE(sharedDirtyPages == expected);
}
}
