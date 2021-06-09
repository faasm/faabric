#include <faabric/util/memory.h>
#include <faabric/util/snapshot.h>
#include <faabric/util/logging.h>

namespace faabric::util {

std::vector<SnapshotDiff> SnapshotData::getDirtyPages()
{
    if (data == nullptr || size == 0) {
        std::vector<faabric::util::SnapshotDiff> empty;
        return empty;
    }

    // Get dirty pages
    int nPages = getRequiredHostPages(size);
    std::vector<bool> dirtyFlags = faabric::util::getDirtyPages(data, nPages);

    // Convert to snapshot diffs
    // TODO - reduce number of diffs by merging adjacent dirty pages
    std::vector<SnapshotDiff> diffs;
    for (int i = 0; i < nPages; i++) {
        if (dirtyFlags.at(i)) {
            uint32_t offset = i * faabric::util::HOST_PAGE_SIZE;
            diffs.emplace_back(
              offset, data + offset, faabric::util::HOST_PAGE_SIZE);
        }
    }

    SPDLOG_DEBUG("Snapshot has {}/{} dirty pages", diffs.size(), nPages);

    return diffs;
}
}
