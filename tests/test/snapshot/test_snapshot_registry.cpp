#include "faabric_utils.h"
#include <catch.hpp>

#include <sys/mman.h>

#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/util/memory.h>

using namespace faabric::snapshot;
using namespace faabric::util;

namespace tests {

TEST_CASE_METHOD(SnapshotTestFixture,
                 "Test set and get snapshots",
                 "[snapshot]")
{
    REQUIRE(reg.getSnapshotCount() == 0);

    std::string keyA = "snapA";
    std::string keyB = "snapB";
    std::string keyC = "snapC";

    SnapshotData snapA = takeSnapshot(keyA, 1, true);
    SnapshotData snapB = takeSnapshot(keyB, 2, false);
    SnapshotData snapC = takeSnapshot(keyA, 3, true);

    // Add some random bits of data to the vectors
    for (int i = 0; i < HOST_PAGE_SIZE - 10; i += 50) {
        snapA.data[i] = i;
        snapB.data[i + 1] = i;
        snapC.data[i + 2] = i;
    }

    reg.takeSnapshot(keyA, snapA);
    reg.takeSnapshot(keyB, snapB, false);
    reg.takeSnapshot(keyC, snapC);

    REQUIRE(reg.getSnapshotCount() == 3);

    SnapshotData actualA = reg.getSnapshot(keyA);
    SnapshotData actualB = reg.getSnapshot(keyB);
    SnapshotData actualC = reg.getSnapshot(keyC);

    REQUIRE(actualA.size == snapA.size);
    REQUIRE(actualB.size == snapB.size);
    REQUIRE(actualC.size == snapC.size);

    // Pointer equality here is good enough
    REQUIRE(actualA.data == snapA.data);
    REQUIRE(actualB.data == snapB.data);
    REQUIRE(actualC.data == snapC.data);

    REQUIRE(actualA.fd > 0);
    REQUIRE(actualB.fd == 0);
    REQUIRE(actualC.fd > 0);

    // Create regions onto which we will map the snapshots
    uint8_t* actualDataA = allocatePages(1);
    uint8_t* actualDataB = allocatePages(2);
    uint8_t* actualDataC = allocatePages(3);

    // Check those that are mappable are mapped
    reg.mapSnapshot(keyA, actualDataA);
    reg.mapSnapshot(keyC, actualDataC);

    // Check error when mapping an unmappable snapshot
    REQUIRE_THROWS(reg.mapSnapshot(keyB, actualDataB));

    // Here we need to check the actual data after mapping
    std::vector<uint8_t> vecDataA(snapA.data, snapA.data + HOST_PAGE_SIZE);
    std::vector<uint8_t> vecActualDataA(actualDataA,
                                        actualDataA + HOST_PAGE_SIZE);
    std::vector<uint8_t> vecDataC(snapC.data,
                                  snapC.data + (3 * HOST_PAGE_SIZE));
    std::vector<uint8_t> vecActualDataC(actualDataC,
                                        actualDataC + (3 * HOST_PAGE_SIZE));

    REQUIRE(vecActualDataA == vecDataA);
    REQUIRE(vecActualDataC == vecDataC);

    removeSnapshot(keyA, 1);
    removeSnapshot(keyB, 2);
    removeSnapshot(keyC, 3);
    deallocatePages(actualDataA, 1);
    deallocatePages(actualDataB, 2);
    deallocatePages(actualDataC, 3);
}
}
