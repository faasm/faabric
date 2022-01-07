#include "faabric/util/dirty.h"
#include "faabric/util/snapshot.h"
#include <faabric/util/bytes.h>
#include <faabric/util/memory.h>
#include <faabric/util/timing.h>

#define MEM_SIZE (100 * 1024 * 1024)

using namespace faabric::util;

void doRun()
{
    MemoryRegion dataA = allocatePrivateMemory(MEM_SIZE);
    MemoryRegion dataB = allocatePrivateMemory(MEM_SIZE);

    // Start tracking one of the bits of memory
    DirtyTracker& tracker = getDirtyTracker();
    std::span<uint8_t> memView(dataA.get(), MEM_SIZE);
    tracker.startTracking(memView);
    tracker.startThreadLocalTracking(memView);

    // Write some data with certain pages containing changes
    bool modifyPage = false;
    bool addDiff = false;
    for (int i = 0; i < MEM_SIZE; i++) {
        if (i % HOST_PAGE_SIZE == 0) {
            modifyPage = !modifyPage;
        }

        if (modifyPage) {
            if (i % 200 == 0) {
                addDiff = !addDiff;
            }

            if (addDiff) {
                dataA.get()[i] = 3;
                dataB.get()[i] = 4;
            } else {
                dataA.get()[i] = 1;
                dataB.get()[i] = 1;
            }
        }
    }

    // Create a snapshot from the other
    auto snap =
      std::make_shared<SnapshotData>(std::span<uint8_t>(dataB.get(), MEM_SIZE));
    snap->fillGapsWithOverwriteRegions();

    tracker.stopTracking(memView);
    tracker.stopThreadLocalTracking(memView);

    auto dirtyRegions = tracker.getBothDirtyOffsets(memView);
    SPDLOG_INFO("Got {} dirty regions", dirtyRegions.size());

    auto diffs = snap->diffWithDirtyRegions(memView, dirtyRegions);
    SPDLOG_INFO("Got {} diffs", diffs.size());
}

int main()
{
    PROF_BEGIN

    doRun();

    PROF_SUMMARY
}
