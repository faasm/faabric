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
        diffs.emplace_back(SnapshotDataType::Raw,
                           SnapshotMergeOperation::Overwrite,
                           offset,
                           data + offset,
                           HOST_PAGE_SIZE);
    }

    SPDLOG_DEBUG("Snapshot has {}/{} dirty pages", diffs.size(), nPages);

    return diffs;
}

std::vector<SnapshotDiff> SnapshotData::getChangeDiffs(const uint8_t* updated,
                                                       size_t updatedSize)
{
    std::vector<SnapshotDiff> diffs;
    if (mergeRegions.empty()) {
        SPDLOG_DEBUG("No merge regions set, thus no diffs");
        return diffs;
    }

    for (const auto& mr : mergeRegions) {
        SPDLOG_TRACE("Merge region {} {} at {}-{}",
                     snapshotDataTypeStr(mr.second.dataType),
                     snapshotMergeOpStr(mr.second.operation),
                     mr.second.offset,
                     mr.second.offset + mr.second.length);
    }

    // Work out which pages have changed (these will be sorted)
    size_t nThisPages = getRequiredHostPages(updatedSize);
    std::vector<int> dirtyPageNumbers =
      getDirtyPageNumbers(updated, nThisPages);

    // Iterate through each dirty page, work out if there's an overlapping merge
    // region, tell that region to add their diffs to the list
    std::map<uint32_t, SnapshotMergeRegion>::iterator mergeIt =
      mergeRegions.begin();

    for (int i : dirtyPageNumbers) {
        int pageStart = i * HOST_PAGE_SIZE;
        int pageEnd = pageStart + HOST_PAGE_SIZE;

        SPDLOG_TRACE("Checking dirty page {} at {}-{}", i, pageStart, pageEnd);

        // Skip any merge regions we've passed
        while (mergeIt != mergeRegions.end() &&
               (mergeIt->second.offset < pageStart)) {
            SPDLOG_TRACE("Gone past {} {} merge region at {}-{}",
                         snapshotDataTypeStr(mergeIt->second.dataType),
                         snapshotMergeOpStr(mergeIt->second.operation),
                         mergeIt->second.offset,
                         mergeIt->second.offset + mergeIt->second.length);

            ++mergeIt;
        }

        if (mergeIt == mergeRegions.end()) {
            // Done if no more merge regions left
            SPDLOG_TRACE("No more merge regions left");
            break;
        }

        // For each merge region that overlaps this dirty page, get it to add
        // its diffs, and move onto the next one
        // TODO - make this more efficient by passing in dirty pages to merge
        // regions so that they avoid unnecessary work if they're large.
        while (mergeIt != mergeRegions.end() &&
               (mergeIt->second.offset >= pageStart &&
                mergeIt->second.offset < pageEnd)) {

            uint8_t* original = data;

            // If we're outside the range of the original data, pass a nullptr
            if (mergeIt->second.offset > size) {
                SPDLOG_TRACE(
                  "Checking {} {} merge region {}-{} outside original snapshot",
                  snapshotDataTypeStr(mergeIt->second.dataType),
                  snapshotMergeOpStr(mergeIt->second.operation),
                  mergeIt->second.offset,
                  mergeIt->second.offset + mergeIt->second.length);

                original = nullptr;
            }

            mergeIt->second.addDiffs(diffs, original, updated);
            mergeIt++;
        }
    }

    return diffs;
}

void SnapshotData::addMergeRegion(uint32_t offset,
                                  size_t length,
                                  SnapshotDataType dataType,
                                  SnapshotMergeOperation operation,
                                  bool overwrite)
{
    SnapshotMergeRegion region{ .offset = offset,
                                .length = length,
                                .dataType = dataType,
                                .operation = operation };

    // Locking as this may be called in bursts by multiple threads
    faabric::util::UniqueLock lock(snapMx);

    if (mergeRegions.find(region.offset) != mergeRegions.end()) {
        if (!overwrite) {
            SPDLOG_ERROR("Attempting to overwrite existing merge region at {} "
                         "with {} {} at {}-{}",
                         region.offset,
                         snapshotDataTypeStr(dataType),
                         snapshotMergeOpStr(operation),
                         region.offset,
                         region.offset + length);

            throw std::runtime_error("Not able to overwrite merge region");
        }

        SPDLOG_TRACE(
          "Overwriting existing merge region at {} with {} {} at {}-{}",
          region.offset,
          snapshotDataTypeStr(dataType),
          snapshotMergeOpStr(operation),
          region.offset,
          region.offset + length);
    } else {
        SPDLOG_DEBUG("Adding new {} {} merge region at {}-{}",
                     snapshotDataTypeStr(dataType),
                     snapshotMergeOpStr(operation),
                     region.offset,
                     region.offset + length);
    }

    mergeRegions[region.offset] = region;
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

void SnapshotMergeRegion::addDiffs(std::vector<SnapshotDiff>& diffs,
                                   const uint8_t* original,
                                   const uint8_t* updated)
{
    SPDLOG_TRACE("Checking for {} {} merge region at {}-{}",
                 snapshotDataTypeStr(dataType),
                 snapshotMergeOpStr(operation),
                 offset,
                 offset + length);

    switch (dataType) {
        case (SnapshotDataType::Int): {
            // Check if the value has changed
            const uint8_t* updatedValue = updated + offset;
            int updatedInt = *(reinterpret_cast<const int*>(updatedValue));

            if (original == nullptr) {
                throw std::runtime_error(
                  "Do not support int operations outside original snapshot");
            }

            const uint8_t* originalValue = original + offset;
            int originalInt = *(reinterpret_cast<const int*>(originalValue));

            // Skip if no change
            if (originalInt == updatedInt) {
                return;
            }

            // Add the diff
            diffs.emplace_back(
              dataType, operation, offset, updatedValue, length);

            SPDLOG_TRACE("Adding {} {} diff at {}-{}",
                         snapshotDataTypeStr(dataType),
                         snapshotMergeOpStr(operation),
                         offset,
                         offset + length);

            // Potentially modify the original in place depending on the
            // operation
            switch (operation) {
                case (SnapshotMergeOperation::Sum): {
                    // Sums must send the value to be _added_, and
                    // not the final result
                    updatedInt -= originalInt;
                    break;
                }
                case (SnapshotMergeOperation::Subtract): {
                    // Subtractions must send the value to be
                    // subtracted, not the result
                    updatedInt = originalInt - updatedInt;
                    break;
                }
                case (SnapshotMergeOperation::Product): {
                    // Products must send the value to be
                    // multiplied, not the result
                    updatedInt /= originalInt;
                    break;
                }
                case (SnapshotMergeOperation::Max):
                case (SnapshotMergeOperation::Min):
                    // Min and max don't need to change
                    break;
                default: {
                    SPDLOG_ERROR("Unhandled integer merge operation: {}",
                                 operation);
                    throw std::runtime_error(
                      "Unhandled integer merge operation");
                }
            }

            // TODO - somehow avoid casting away the const here?
            // Modify the memory in-place here
            std::memcpy(
              (uint8_t*)updatedValue, BYTES(&updatedInt), sizeof(int32_t));

            break;
        }
        case (SnapshotDataType::Raw): {
            switch (operation) {
                case (SnapshotMergeOperation::Overwrite): {
                    // Add subsections of diffs only for the bytes that
                    // have changed
                    bool diffInProgress = false;
                    int diffStart = 0;
                    for (int b = offset; b <= offset + length; b++) {
                        bool isDirtyByte = false;

                        if (original == nullptr) {
                            isDirtyByte = true;
                        } else {
                            isDirtyByte = *(original + b) != *(updated + b);
                        }

                        SPDLOG_TRACE("BYTE {} dirty {}", b, isDirtyByte);
                        if (isDirtyByte && !diffInProgress) {
                            // Diff starts here if it's different and diff
                            // not in progress
                            diffInProgress = true;
                            diffStart = b;
                        } else if (!isDirtyByte && diffInProgress) {
                            // Diff ends if it's not different and diff is
                            // in progress
                            int diffLength = b - diffStart;
                            SPDLOG_TRACE("Adding {} {} diff at {}-{}",
                                         snapshotDataTypeStr(dataType),
                                         snapshotMergeOpStr(operation),
                                         diffStart,
                                         diffStart + diffLength);

                            diffInProgress = false;
                            diffs.emplace_back(dataType,
                                               operation,
                                               diffStart,
                                               updated + diffStart,
                                               diffLength);
                        }
                    }

                    // If we've reached the end of this region with a diff
                    // in progress, we need to close it off
                    if (diffInProgress) {
                        int finalDiffLength = (offset + length) - diffStart + 1;
                        SPDLOG_TRACE(
                          "Adding {} {} diff at {}-{} (end of region)",
                          snapshotDataTypeStr(dataType),
                          snapshotMergeOpStr(operation),
                          diffStart,
                          diffStart + finalDiffLength);

                        diffs.emplace_back(dataType,
                                           operation,
                                           diffStart,
                                           updated + diffStart,
                                           finalDiffLength);
                    }
                    break;
                }
                default: {
                    SPDLOG_ERROR("Unhandled raw merge operation: {}",
                                 operation);
                    throw std::runtime_error("Unhandled raw merge operation");
                }
            }

            break;
        }
        default: {
            SPDLOG_ERROR("Merge region for unhandled data type: {}", dataType);
            throw std::runtime_error("Merge region for unhandled data type");
        }
    }
}
}
