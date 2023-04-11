#include <catch2/catch.hpp>

#include "faabric_utils.h"
#include "fixtures.h"

#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/util/memory.h>

#include <sys/mman.h>

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

    REQUIRE(!reg.snapshotExists(keyA));
    REQUIRE(!reg.snapshotExists(keyB));
    REQUIRE(!reg.snapshotExists(keyC));

    auto snapA = setUpSnapshot(keyA, 1);
    auto snapB = setUpSnapshot(keyB, 2);

    REQUIRE(reg.snapshotExists(keyA));
    REQUIRE(reg.snapshotExists(keyB));
    REQUIRE(!reg.snapshotExists(keyC));
    REQUIRE(reg.getSnapshotCount() == 2);

    auto snapC = setUpSnapshot(keyC, 3);

    REQUIRE(reg.snapshotExists(keyA));
    REQUIRE(reg.snapshotExists(keyB));
    REQUIRE(reg.snapshotExists(keyC));
    REQUIRE(reg.getSnapshotCount() == 3);

    // Add some random bits of data to the snapshots
    for (int i = 0; i < HOST_PAGE_SIZE - 10; i += 50) {
        snapA->copyInData({ BYTES(&i), sizeof(int) }, i);
        snapB->copyInData({ BYTES(&i), sizeof(int) }, i + 1);
        snapC->copyInData({ BYTES(&i), sizeof(int) }, i + 2);
    }

    // Take snapshots again with updated data
    reg.registerSnapshot(keyA, snapA);
    reg.registerSnapshot(keyB, snapB);
    reg.registerSnapshot(keyC, snapC);

    auto actualA = reg.getSnapshot(keyA);
    auto actualB = reg.getSnapshot(keyB);
    auto actualC = reg.getSnapshot(keyC);

    REQUIRE(actualA->getSize() == snapA->getSize());
    REQUIRE(actualB->getSize() == snapB->getSize());
    REQUIRE(actualC->getSize() == snapC->getSize());

    // Pointer equality here is good enough
    REQUIRE(actualA->getDataPtr() == snapA->getDataPtr());
    REQUIRE(actualB->getDataPtr() == snapB->getDataPtr());
    REQUIRE(actualC->getDataPtr() == snapC->getDataPtr());

    // Create regions onto which we will map the snapshots
    size_t sizeA = HOST_PAGE_SIZE;
    size_t sizeC = 3 * HOST_PAGE_SIZE;
    MemoryRegion actualDataA = allocatePrivateMemory(sizeA);
    MemoryRegion actualDataB = allocatePrivateMemory(2 * HOST_PAGE_SIZE);
    MemoryRegion actualDataC = allocatePrivateMemory(sizeC);

    // Map two of them
    snapA->mapToMemory({ actualDataA.get(), sizeA });
    snapC->mapToMemory({ actualDataC.get(), sizeC });

    // Here we need to check the actual data after mapping
    std::vector<uint8_t> vecDataA = snapA->getDataCopy();
    std::vector<uint8_t> vecActualDataA(actualDataA.get(),
                                        actualDataA.get() + HOST_PAGE_SIZE);

    std::vector<uint8_t> vecDataC = snapC->getDataCopy();
    std::vector<uint8_t> vecActualDataC(
      actualDataC.get(), actualDataC.get() + (3 * HOST_PAGE_SIZE));

    REQUIRE(vecActualDataA == vecDataA);
    REQUIRE(vecActualDataC == vecDataC);

    removeSnapshot(keyA, 1);
    removeSnapshot(keyB, 2);

    REQUIRE(!reg.snapshotExists(keyA));
    REQUIRE(!reg.snapshotExists(keyB));
    REQUIRE(reg.snapshotExists(keyC));
    REQUIRE(reg.getSnapshotCount() == 1);

    removeSnapshot(keyC, 3);

    REQUIRE(!reg.snapshotExists(keyA));
    REQUIRE(!reg.snapshotExists(keyB));
    REQUIRE(!reg.snapshotExists(keyC));
    REQUIRE(reg.getSnapshotCount() == 0);
}

TEST_CASE_METHOD(SnapshotTestFixture,
                 "Test can't get snapshot with empty key",
                 "[snapshot]")
{
    REQUIRE_THROWS(reg.getSnapshot(""));
}
}
