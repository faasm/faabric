#include <faabric/util/locks.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>
#include <faabric/util/memory.h>
#include <faabric/util/snapshot.h>

namespace faabric::util {

// TODO - this would be better as an instance variable on the SnapshotData
// class, but it can't be copy-constructed.
static std::mutex snapMx;

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
    for (auto& m : mergeRegions) {
        SPDLOG_TRACE("{} {} merge region at {} {}-{}",
                     snapshotDataTypeStr(m.second.dataType),
                     snapshotMergeOpStr(m.second.operation),
                     m.first,
                     m.second.offset,
                     m.second.offset + m.second.length);
    }

    std::vector<SnapshotDiff> diffs;
    if (_isCustomMerge) {
        diffs = getCustomDiffs(updated, updatedSize);
    } else {
        diffs = getStandardDiffs(updated, updatedSize);
    }

    return diffs;
}

std::vector<SnapshotDiff> SnapshotData::getCustomDiffs(const uint8_t* updated,
                                                       size_t updatedSize)
{
    SPDLOG_TRACE("Doing custom diff with {} merge regions",
                 mergeRegions.size());

    // Here we ignore all diffs except for those specified in merge regions
    std::vector<SnapshotDiff> diffs;
    for (auto& mergePair : mergeRegions) {
        // Get the diff for this merge region
        SnapshotMergeRegion region = mergePair.second;
        SnapshotDiff diff = region.toDiff(data, updated);

        if (diff.noChange) {
            SPDLOG_TRACE("No diff for {} {} merge region at {} {}-{}",
                         snapshotDataTypeStr(mergePair.second.dataType),
                         snapshotMergeOpStr(mergePair.second.operation),
                         mergePair.first,
                         mergePair.second.offset,
                         mergePair.second.offset + mergePair.second.length);
            continue;
        }

        diffs.push_back(diff);
    }

    return diffs;
}

std::vector<SnapshotDiff> SnapshotData::getStandardDiffs(const uint8_t* updated,
                                                         size_t updatedSize)
{
    // Work out which pages have changed
    size_t nThisPages = getRequiredHostPages(size);
    std::vector<int> dirtyPageNumbers =
      getDirtyPageNumbers(updated, nThisPages);

    SPDLOG_TRACE("Diffing {} pages with {} changed pages and {} merge regions",
                 nThisPages,
                 dirtyPageNumbers.size(),
                 mergeRegions.size());

    // Get iterator over merge regions
    std::map<uint32_t, SnapshotMergeRegion>::iterator mergeIt =
      mergeRegions.begin();

    // Get byte-wise diffs _within_ the dirty pages
    //
    // NOTE - if raw diffs cover page boundaries, they will be split into
    // multiple diffs, each of which is page-aligned.
    // We can be relatively confident that variables will be page-aligned so
    // this shouldn't be a problem.
    //
    // Merge regions support crossing page boundaries.
    //
    // For each byte we encounter have the following possible scenarios:
    //
    // 1. the byte is dirty, and is the start of a new diff
    // 2. the byte is dirty, but the byte before was also dirty, so we
    // are inside a diff
    // 3. the byte is not dirty but the previous one was, so we've reached the
    // end of a diff
    // 4. the last byte of the page is dirty, so we've also come to the end of
    // a diff
    // 5. the byte is dirty, but is within a special merge region, in which
    // case we need to add a diff for that whole region, then skip
    // to the next byte after that region
    std::vector<SnapshotDiff> diffs;
    for (int i : dirtyPageNumbers) {
        int pageOffset = i * HOST_PAGE_SIZE;

        bool diffInProgress = false;
        int diffStart = 0;
        int offset = pageOffset;
        for (int b = 0; b < HOST_PAGE_SIZE; b++) {
            offset = pageOffset + b;
            bool isDirtyByte = *(data + offset) != *(updated + offset);

            // Skip any merge regions we've passed
            while (mergeIt != mergeRegions.end() &&
                   offset >=
                     (mergeIt->second.offset + mergeIt->second.length)) {
                SnapshotMergeRegion region = mergeIt->second;
                SPDLOG_TRACE("At offset {}, past region {} {} {}-{}",
                             offset,
                             snapshotDataTypeStr(region.dataType),
                             snapshotMergeOpStr(region.operation),
                             region.offset,
                             region.offset + region.length);

                ++mergeIt;
            }

            // Check if we're in a merge region
            bool isInMergeRegion =
              mergeIt != mergeRegions.end() &&
              offset >= mergeIt->second.offset &&
              offset < (mergeIt->second.offset + mergeIt->second.length);

            if (isDirtyByte && isInMergeRegion) {
                // If we've entered a merge region with a diff in progress, we
                // need to close it off
                if (diffInProgress) {
                    diffs.emplace_back(
                      diffStart, updated + diffStart, offset - diffStart);

                    SPDLOG_TRACE(
                      "Finished {} {} diff between {}-{} before merge region",
                      snapshotDataTypeStr(diffs.back().dataType),
                      snapshotMergeOpStr(diffs.back().operation),
                      diffs.back().offset,
                      diffs.back().offset + diffs.back().size);

                    diffInProgress = false;
                }

                // Get the diff for this merge region
                SnapshotMergeRegion region = mergeIt->second;
                SnapshotDiff diff = region.toDiff(data, updated);

                SPDLOG_TRACE("Diff at {} falls in {} {} merge region {}-{}",
                             pageOffset + b,
                             snapshotDataTypeStr(region.dataType),
                             snapshotMergeOpStr(region.operation),
                             region.offset,
                             region.offset + region.length);

                // Add the diff to the list
                if (diff.operation != SnapshotMergeOperation::Ignore) {
                    diffs.emplace_back(diff);
                }

                // Work out the offset where this region ends
                int regionEndOffset =
                  (region.offset - pageOffset) + region.length;

                if (regionEndOffset < HOST_PAGE_SIZE) {
                    // Skip over this region, still more offsets left in this
                    // page
                    SPDLOG_TRACE(
                      "{} {} merge region {}-{} finished. Skipping to {}",
                      snapshotDataTypeStr(region.dataType),
                      snapshotMergeOpStr(region.operation),
                      region.offset,
                      region.offset + region.length,
                      pageOffset + regionEndOffset);

                    // Bump the loop variable to the end of this region (note
                    // that the loop itself will increment onto the next).
                    b = regionEndOffset - 1;
                } else {
                    // Merge region extends over this page, move onto next
                    SPDLOG_TRACE(
                      "{} {} merge region {}-{} over page boundary {} ({}-{})",
                      snapshotDataTypeStr(region.dataType),
                      snapshotMergeOpStr(region.operation),
                      region.offset,
                      region.offset + region.length,
                      i,
                      pageOffset,
                      pageOffset + HOST_PAGE_SIZE);

                    break;
                }
            } else if (isDirtyByte && !diffInProgress) {
                // Diff starts here if it's different and diff not in progress
                diffInProgress = true;
                diffStart = offset;

                SPDLOG_TRACE("Started Raw Overwrite diff at {}", diffStart);
            } else if (!isDirtyByte && diffInProgress) {
                // Diff ends if it's not different and diff is in progress
                diffInProgress = false;
                diffs.emplace_back(
                  diffStart, updated + diffStart, offset - diffStart);

                SPDLOG_TRACE("Finished {} {} diff between {}-{}",
                             snapshotDataTypeStr(diffs.back().dataType),
                             snapshotMergeOpStr(diffs.back().operation),
                             diffs.back().offset,
                             diffs.back().offset + diffs.back().size);
            }
        }

        // If we've reached the end of this page with a diff in progress, we
        // need to close it off
        if (diffInProgress) {
            offset++;

            diffs.emplace_back(
              diffStart, updated + diffStart, offset - diffStart);

            SPDLOG_TRACE("Found {} {} diff between {}-{} at end of page",
                         snapshotDataTypeStr(diffs.back().dataType),
                         snapshotMergeOpStr(diffs.back().operation),
                         diffs.back().offset,
                         diffs.back().offset + diffs.back().size);
        }
    }

    // If comparison has more pages than the original, add another diff
    // containing all the new pages
    if (updatedSize > size) {
        diffs.emplace_back(size, updated + size, updatedSize - size);
    }

    return diffs;
}

void SnapshotData::addMergeRegion(uint32_t offset,
                                  size_t length,
                                  SnapshotDataType dataType,
                                  SnapshotMergeOperation operation)
{
    SnapshotMergeRegion region{ .offset = offset,
                                .length = length,
                                .dataType = dataType,
                                .operation = operation };

    // Locking as this may be called in bursts by multiple threads
    faabric::util::UniqueLock lock(snapMx);
    mergeRegions[offset] = region;
}

void SnapshotData::setIsCustomMerge(bool value)
{
    _isCustomMerge = value;
}

bool SnapshotData::isCustomMerge()
{
    return _isCustomMerge;
}

std::string snapshotDataTypeStr(SnapshotDataType dt)
{
    switch (dt) {
        case (SnapshotDataType::Raw): {
            return "Raw";
        }
        case (SnapshotDataType::Int): {
            return "Int";
        }
        default: {
            SPDLOG_ERROR("Cannot convert snapshot data type to string: {}", dt);
            throw std::runtime_error("Cannot convert data type to string");
        }
    }
}

std::string snapshotMergeOpStr(SnapshotMergeOperation op)
{
    switch (op) {
        case (SnapshotMergeOperation::Ignore): {
            return "Ignore";
        }
        case (SnapshotMergeOperation::Max): {
            return "Max";
        }
        case (SnapshotMergeOperation::Min): {
            return "Min";
        }
        case (SnapshotMergeOperation::Overwrite): {
            return "Overwrite";
        }
        case (SnapshotMergeOperation::Product): {
            return "Product";
        }
        case (SnapshotMergeOperation::Subtract): {
            return "Subtract";
        }
        case (SnapshotMergeOperation::Sum): {
            return "Sum";
        }
        default: {
            SPDLOG_ERROR("Cannot convert snapshot merge op to string: {}", op);
            throw std::runtime_error("Cannot convert merge op to string");
        }
    }
}
}
