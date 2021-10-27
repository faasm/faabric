#include "faabric_utils.h"
#include <catch2/catch.hpp>

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

    SnapshotData snapA = takeSnapshot(keyA, 1, true);
    SnapshotData snapB = takeSnapshot(keyB, 2, false);

    REQUIRE(reg.snapshotExists(keyA));
    REQUIRE(reg.snapshotExists(keyB));
    REQUIRE(!reg.snapshotExists(keyC));
    REQUIRE(reg.getSnapshotCount() == 2);

    SnapshotData snapC = takeSnapshot(keyC, 3, true);

    REQUIRE(reg.snapshotExists(keyA));
    REQUIRE(reg.snapshotExists(keyB));
    REQUIRE(reg.snapshotExists(keyC));
    REQUIRE(reg.getSnapshotCount() == 3);

    // Add some random bits of data to the vectors
    for (int i = 0; i < HOST_PAGE_SIZE - 10; i += 50) {
        snapA.data[i] = i;
        snapB.data[i + 1] = i;
        snapC.data[i + 2] = i;
    }

    // Take snapshots again with updated data
    reg.takeSnapshot(keyA, snapA);
    reg.takeSnapshot(keyB, snapB, false);
    reg.takeSnapshot(keyC, snapC);

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

    REQUIRE(!reg.snapshotExists(keyA));
    REQUIRE(!reg.snapshotExists(keyB));
    REQUIRE(reg.snapshotExists(keyC));
    REQUIRE(reg.getSnapshotCount() == 1);

    removeSnapshot(keyC, 3);

    REQUIRE(!reg.snapshotExists(keyA));
    REQUIRE(!reg.snapshotExists(keyB));
    REQUIRE(!reg.snapshotExists(keyC));
    REQUIRE(reg.getSnapshotCount() == 0);

    deallocatePages(actualDataA, 1);
    deallocatePages(actualDataB, 2);
    deallocatePages(actualDataC, 3);
}

TEST_CASE_METHOD(SnapshotTestFixture,
                 "Test set snapshot if not exists",
                 "[snapshot]")
{
    REQUIRE(reg.getSnapshotCount() == 0);

    std::string keyA = "snapA";
    std::string keyB = "snapB";

    REQUIRE(!reg.snapshotExists(keyA));
    REQUIRE(!reg.snapshotExists(keyB));

    // Take one of the snapshots
    SnapshotData snapBefore = takeSnapshot(keyA, 1, true);

    REQUIRE(reg.snapshotExists(keyA));
    REQUIRE(!reg.snapshotExists(keyB));
    REQUIRE(reg.getSnapshotCount() == 1);

    // Set up some different data
    std::vector<uint8_t> otherDataA(snapBefore.size + 10, 1);
    std::vector<uint8_t> otherDataB(snapBefore.size + 5, 2);

    SnapshotData snapUpdateA;
    snapUpdateA.data = otherDataA.data();
    snapUpdateA.size = otherDataA.size();

    SnapshotData snapUpdateB;
    snapUpdateB.data = otherDataB.data();
    snapUpdateB.size = otherDataB.size();

    // Check existing snapshot is not overwritten
    reg.takeSnapshotIfNotExists(keyA, snapUpdateA, true);
    SnapshotData snapAfterA = reg.getSnapshot(keyA);
    REQUIRE(snapAfterA.data == snapBefore.data);
    REQUIRE(snapAfterA.size == snapBefore.size);

    // Check new snapshot is still created
    reg.takeSnapshotIfNotExists(keyB, snapUpdateB, true);

    REQUIRE(reg.snapshotExists(keyA));
    REQUIRE(reg.snapshotExists(keyB));
    REQUIRE(reg.getSnapshotCount() == 2);

    SnapshotData snapAfterB = reg.getSnapshot(keyB);
    REQUIRE(snapAfterB.data == otherDataB.data());
    REQUIRE(snapAfterB.size == otherDataB.size());
}

TEST_CASE_METHOD(SnapshotTestFixture,
                 "Test can't get snapshot with empty key",
                 "[snapshot]")
{
    REQUIRE_THROWS(reg.getSnapshot(""));
}
}
