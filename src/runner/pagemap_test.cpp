#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/util/logging.h>
#include <faabric/util/memory.h>

#include <sys/mman.h>

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

int main()
{
    faabric::util::initLogging();
    faabric::snapshot::SnapshotRegistry& reg =
      faabric::snapshot::getSnapshotRegistry();

    std::string snapKey = "dirty-check-snap";

    int nPages = 4;

    faabric::util::SnapshotData snap;
    snap.data = allocatePages(nPages);
    snap.size = nPages * faabric::util::HOST_PAGE_SIZE;
    reg.takeSnapshot(snapKey, snap, true);

    uint8_t* sharedMem = allocatePages(nPages);

    // Reset tracking before mapping
    faabric::util::resetDirtyTracking();

    // Map the memory
    uint8_t* mappedMem = reg.mapSnapshot(snapKey, sharedMem);

    if (mappedMem != sharedMem) {
        SPDLOG_ERROR("Mapped region is not snap region");
    }

    // Mapping will cause dirty pages on only the mapped region
    std::vector<faabric::util::SnapshotDiff> snapDiffs = snap.getDirtyPages();
    if (!snapDiffs.empty()) {
        SPDLOG_ERROR("Got {} dirty pages on original snap", snapDiffs.size());
    }

    printf("AFTER MAPPING\n");
    faabric::util::printFlags(sharedMem, nPages);

    faabric::util::resetDirtyTracking();
    printf("AFTER CLEARING\n");
    faabric::util::printFlags(sharedMem, nPages);

    *(sharedMem + faabric::util::HOST_PAGE_SIZE + 2) = 1;
    printf("AFTER WRITING\n");
    faabric::util::printFlags(sharedMem, nPages);

    // Tidy up
    deallocatePages(sharedMem, nPages);

    return 0;
}
