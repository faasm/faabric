#include "faabric/util/dirty.h"
#include "faabric/util/snapshot.h"
#include <faabric/util/bytes.h>
#include <faabric/util/memory.h>
#include <faabric/util/timing.h>

#define MEM_SIZE (4 * 1024 * 1024)

using namespace faabric::util;

void doRun()
{
    std::vector<uint8_t> dataA(MEM_SIZE, 0);
    std::vector<uint8_t> dataB(MEM_SIZE, 0);

    // Start tracking one of the bits of memory
    DirtyTracker& tracker = getDirtyTracker();
    tracker.startTracking(dataA);
    tracker.startThreadLocalTracking(dataA);

    // Write some data with certain pages containing changes
    bool modifyPages = false;
    bool smallChanges = false;
    for (int i = 0; i < MEM_SIZE; i++) {
        if (i % HOST_PAGE_SIZE == 0) {
            modifyPages = !modifyPages;
        }

        if (modifyPages) {
            if (i % 200 == 0) {
                smallChanges = !smallChanges;
            }

            if (smallChanges) {
                dataA.at(i) = 3;
                dataB.at(i) = 4;
            } else {
                dataA.at(i) = 1;
                dataB.at(i) = 1;
            }
        }
    }

    // Create a snapshot from the other
    auto snap = std::make_shared<SnapshotData>(dataB);

    tracker.stopTracking(dataA);
    tracker.stopThreadLocalTracking(dataA);

    auto dirtyRegions = tracker.getBothDirtyOffsets(dataA);
    SPDLOG_INFO("Got {} dirty regions", dirtyRegions.size());

    auto diffs = snap->diffWithDirtyRegions(dataA, dirtyRegions);
    SPDLOG_INFO("Got {} diffs", diffs.size());
}

int main()
{

    PROF_BEGIN

    doRun();

    PROF_SUMMARY
}
