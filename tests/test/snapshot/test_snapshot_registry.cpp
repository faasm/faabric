#include "faabric_utils.h"
#include <catch.hpp>

#include <sys/mman.h>

#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/util/memory.h>

using namespace faabric::snapshot;
using namespace faabric::util;

namespace tests {

uint8_t* allocatePages(int nPages)
{
    return (uint8_t*)mmap(nullptr,
                          nPages * HOST_PAGE_SIZE,
                          PROT_WRITE,
                          MAP_SHARED | MAP_ANONYMOUS,
                          -1,
                          0);
}
void deallocatePages(uint8_t* base, int nPages)
{
    munmap(base, nPages * HOST_PAGE_SIZE);
}

TEST_CASE("Test set and get snapshots", "[snapshot]")
{
    cleanFaabric();

    SnapshotRegistry& reg = getSnapshotRegistry();

    REQUIRE(reg.getSnapshotCount() == 0);

    SnapshotData snapA;
    SnapshotData snapB;
    SnapshotData snapC;

    // Snapshot have to be page-aligned
    uint8_t* dataA = allocatePages(1);
    uint8_t* dataB = allocatePages(2);
    uint8_t* dataC = allocatePages(3);

    // Add some random bits of data to the vectors
    for (int i = 0; i < HOST_PAGE_SIZE - 10; i += 50) {
        dataA[i] = i;
        dataB[i + 1] = i;
        dataC[i + 2] = i;
    }

    snapA.size = HOST_PAGE_SIZE;
    snapA.data = dataA;

    snapB.size = HOST_PAGE_SIZE;
    snapB.data = dataB;

    snapC.size = HOST_PAGE_SIZE;
    snapC.data = dataC;

    std::string keyA = "snapA";
    std::string keyB = "snapB";
    std::string keyC = "snapC";

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
    std::vector<uint8_t> vecDataA(dataA, dataA + HOST_PAGE_SIZE);
    std::vector<uint8_t> vecActualDataA(actualDataA,
                                        actualDataA + HOST_PAGE_SIZE);
    std::vector<uint8_t> vecDataC(dataC, dataC + (3 * HOST_PAGE_SIZE));
    std::vector<uint8_t> vecActualDataC(actualDataC,
                                        actualDataC + (3 * HOST_PAGE_SIZE));

    REQUIRE(vecActualDataA == vecDataA);
    REQUIRE(vecActualDataC == vecDataC);

    deallocatePages(dataA, 1);
    deallocatePages(actualDataA, 1);
    deallocatePages(dataB, 2);
    deallocatePages(actualDataB, 2);
    deallocatePages(dataC, 3);
    deallocatePages(actualDataC, 3);
}
:x
:
