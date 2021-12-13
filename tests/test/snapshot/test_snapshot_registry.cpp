#include <catch2/catch.hpp>

#include "faabric_utils.h"

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

    uint8_t* rawA = snapA->getMutableDataPtr();
    uint8_t* rawB = snapB->getMutableDataPtr();
    uint8_t* rawC = snapC->getMutableDataPtr();

    // Add some random bits of data to the vectors
    for (int i = 0; i < HOST_PAGE_SIZE - 10; i += 50) {
        rawA[i] = i;
        rawB[i + 1] = i;
        rawC[i + 2] = i;
    }

    // Make two restorable
    snapA->makeRestorable(keyA);
    snapC->makeRestorable(keyC);

    // Take snapshots again with updated data
    reg.registerSnapshot(keyA, snapA);
    reg.registerSnapshot(keyB, snapB);
    reg.registerSnapshot(keyC, snapC);

    auto actualA = reg.getSnapshot(keyA);
    auto actualB = reg.getSnapshot(keyB);
    auto actualC = reg.getSnapshot(keyC);

    REQUIRE(actualA->size == snapA->size);
    REQUIRE(actualB->size == snapB->size);
    REQUIRE(actualC->size == snapC->size);

    // Pointer equality here is good enough
    REQUIRE(actualA->getDataPtr() == snapA->getDataPtr());
    REQUIRE(actualB->getDataPtr() == snapB->getDataPtr());
    REQUIRE(actualC->getDataPtr() == snapC->getDataPtr());

    REQUIRE(actualA->isRestorable());
    REQUIRE(!actualB->isRestorable());
    REQUIRE(actualC->isRestorable());

    // Create regions onto which we will map the snapshots
    MemoryRegion actualDataA = allocateSharedMemory(1 * HOST_PAGE_SIZE);
    MemoryRegion actualDataB = allocateSharedMemory(2 * HOST_PAGE_SIZE);
    MemoryRegion actualDataC = allocateSharedMemory(3 * HOST_PAGE_SIZE);

    // Check those that are mappable are mapped
    reg.mapSnapshot(keyA, actualDataA.get());
    reg.mapSnapshot(keyC, actualDataC.get());

    // Check error when mapping an unmappable snapshot
    REQUIRE_THROWS(reg.mapSnapshot(keyB, actualDataB.get()));

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
                 "Test register snapshot if not exists",
                 "[snapshot]")
{
    REQUIRE(reg.getSnapshotCount() == 0);

    std::string keyA = "snapA";
    std::string keyB = "snapB";

    REQUIRE(!reg.snapshotExists(keyA));
    REQUIRE(!reg.snapshotExists(keyB));

    // Take one of the snapshots
    auto snapBefore = setUpSnapshot(keyA, 1);

    REQUIRE(reg.snapshotExists(keyA));
    REQUIRE(!reg.snapshotExists(keyB));
    REQUIRE(reg.getSnapshotCount() == 1);

    auto otherSnap =
      std::make_shared<faabric::util::SnapshotData>(snapBefore->size + 10);
    std::vector<uint8_t> otherData(snapBefore->size + 10, 1);
    otherSnap->copyInData(otherData);

    // Check existing snapshot is not overwritten
    reg.registerSnapshotIfNotExists(keyA, otherSnap);
    auto snapAfterA = reg.getSnapshot(keyA);
    REQUIRE(snapAfterA->getDataPtr() == snapBefore->getDataPtr());
    REQUIRE(snapAfterA->size == snapBefore->size);

    // Check new snapshot is still created
    reg.registerSnapshotIfNotExists(keyB, otherSnap);

    REQUIRE(reg.snapshotExists(keyA));
    REQUIRE(reg.snapshotExists(keyB));
    REQUIRE(reg.getSnapshotCount() == 2);

    auto snapAfterB = reg.getSnapshot(keyB);
    REQUIRE(snapAfterB->getDataPtr() == otherSnap->getDataPtr());
    REQUIRE(snapAfterB->size == otherSnap->size);
}

TEST_CASE_METHOD(SnapshotTestFixture,
                 "Test can't get snapshot with empty key",
                 "[snapshot]")
{
    REQUIRE_THROWS(reg.getSnapshot(""));
}
}
