#include <faabric/util/logging.h>
#include <faabric/util/memory.h>
#include <faabric/util/snapshot.h>

namespace faabric::util {

SnapshotData::SnapshotData()
  : merger(std::make_shared<SnapshotDiffMerger>())
{}

std::vector<SnapshotDiff> SnapshotData::getDirtyPages()
{
    if (data == nullptr || size == 0) {
        std::vector<SnapshotDiff> empty;
        return empty;
    }

    // Get dirty pages
    int nPages = getRequiredHostPages(size);
    std::vector<int> dirtyPageNumbers = getDirtyPageNumbers(data, nPages);

    // Convert to snapshot diffs
    // TODO - reduce number of diffs by merging adjacent dirty pages
    std::vector<SnapshotDiff> diffs;
    for (int i : dirtyPageNumbers) {
        uint32_t offset = i * HOST_PAGE_SIZE;
        diffs.emplace_back(offset, data + offset, HOST_PAGE_SIZE);
    }

    SPDLOG_DEBUG("Snapshot has {}/{} dirty pages", diffs.size(), nPages);

    return diffs;
}

std::vector<SnapshotDiff> SnapshotData::getChangeDiffs(const uint8_t* updated,
                                                       size_t updatedSize)
{
    // Work out which pages have changed in the comparison
    size_t nThisPages = getRequiredHostPages(size);
    std::vector<int> dirtyPageNumbers =
      getDirtyPageNumbers(updated, nThisPages);

    // Get byte-wise diffs _within_ the dirty pages
    // NOTE - this will cause diffs to be split across pages if they hit a page
    // boundary, but we can be relatively confident that variables will be
    // page-aligned so this shouldn't be a problem
    std::vector<SnapshotDiff> diffs;
    for (int i : dirtyPageNumbers) {
        int pageOffset = i * HOST_PAGE_SIZE;

        // Iterate through each byte of the page
        bool diffInProgress = false;
        int diffStart = 0;
        int offset = pageOffset;
        for (int b = 0; b < HOST_PAGE_SIZE; b++) {
            offset = pageOffset + b;
            bool isDirtyByte = *(data + offset) != *(updated + offset);
            if (isDirtyByte && !diffInProgress) {
                // Diff starts here if it's different and diff not in progress
                diffInProgress = true;
                diffStart = offset;
            } else if (!isDirtyByte && diffInProgress) {
                // Diff ends if it's not different and diff is in progress
                diffInProgress = false;
                diffs.emplace_back(
                  diffStart, updated + diffStart, offset - diffStart);
            }
        }

        // If we've reached the end with a diff in progress, we need to close it
        // off
        if (diffInProgress) {
            offset++;
            diffs.emplace_back(
              diffStart, updated + diffStart, offset - diffStart);
        }
    }

    // If comparison has more pages than the original, add another diff
    // containing all the new pages
    if (updatedSize > size) {
        diffs.emplace_back(size, updated + size, updatedSize - size);
    }

    return diffs;
}

void SnapshotDiffMerger::applyDiff(size_t diffOffset,
                                   const uint8_t* diffData,
                                   size_t diffLen,
                                   uint8_t* targetBase)
{
    uint8_t* dest = targetBase + diffOffset;
    std::memcpy(dest, diffData, diffLen);
};

void SnapshotData::applyDiff(size_t diffOffset,
                             const uint8_t* diffData,
                             size_t diffLen)
{
    getMerger()->applyDiff(diffOffset, diffData, diffLen, data);
}

std::shared_ptr<SnapshotDiffMerger> SnapshotData::getMerger()
{
    return merger;
}

void SnapshotData::setMerger(std::shared_ptr<SnapshotDiffMerger> mergerIn)
{
    merger = mergerIn;
}
}
