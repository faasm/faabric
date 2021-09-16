#include <faabric/util/logging.h>
#include <faabric/util/memory.h>
#include <faabric/util/snapshot.h>

namespace faabric::util {

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
    // Work out which pages have changed
    size_t nThisPages = getRequiredHostPages(size);
    std::vector<int> dirtyPageNumbers =
      getDirtyPageNumbers(updated, nThisPages);

    // Get iterator over merge regions
    std::map<uint32_t, SnapshotMergeRegion>::iterator mergeIt =
      mergeRegions.begin();

    // Get byte-wise diffs _within_ the dirty pages
    //
    // NOTE - this will cause diffs to be split across pages if they hit a page
    // boundary, but we can be relatively confident that variables will be
    // page-aligned so this shouldn't be a problem
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

            if (isDirtyByte && mergeIt != mergeRegions.end() &&
                offset >= mergeIt->second.offset &&
                offset <= (mergeIt->second.offset + mergeIt->second.length)) {

                // Set up the diff
                const uint8_t* updatedData = updated + mergeIt->second.offset;
                const uint8_t* originalData = data + mergeIt->second.offset;
                SnapshotDiff diff(
                  mergeIt->second.offset, updatedData, mergeIt->second.length);
                diff.dataType = mergeIt->second.dataType;
                diff.operation = mergeIt->second.operation;

                // Modify diff data for certain operations
                switch (mergeIt->second.dataType) {
                    case (SnapshotDataType::Int): {
                        const int* original =
                          reinterpret_cast<const int*>(originalData);
                        const int* updated =
                          reinterpret_cast<const int*>(updatedData);

                        switch (mergeIt->second.operation) {
                            case (SnapshotMergeOperation::Sum): {
                                // Sums must send the value to be _added_, and
                                // not the final result
                                int change = *updated - *original;
                                std::memcpy(
                                  (int*)updated, &change, sizeof(int32_t));
                                break;
                            }
                            case (SnapshotMergeOperation::Subtract): {
                                // Subtractions must send the value to be
                                // subtracted, not the result
                                int change = *original - *updated;
                                std::memcpy(
                                  (int*)updated, &change, sizeof(int32_t));
                                break;
                            }
                            case (SnapshotMergeOperation::Product): {
                                // Products must send the value to be
                                // multiplied, not the result
                                int change = *updated / *original;
                                std::memcpy(
                                  (int*)updated, &change, sizeof(int32_t));
                                break;
                            }
                            case (SnapshotMergeOperation::Max):
                            case (SnapshotMergeOperation::Min):
                                // Min and max don't need to change
                                break;
                            default: {
                                SPDLOG_ERROR(
                                  "Unhandled integer merge operation: {}",
                                  mergeIt->second.operation);
                                throw std::runtime_error(
                                  "Unhandled integer merge operation");
                            }
                        }
                        break;
                    }
                    case (SnapshotDataType::Raw): {
                        // Do nothing for raw data
                        break;
                    }
                    default: {
                        SPDLOG_ERROR("Merge region for unhandled data type: {}",
                                     mergeIt->second.dataType);
                        throw std::runtime_error(
                          "Merge region for unhandled data type");
                    }
                }

                // Add the diff to the list
                diffs.emplace_back(diff);

                // Bump the loop variable to the next byte after this region
                int nextOffset =
                  mergeIt->second.offset + mergeIt->second.length + 1;
                int jump = nextOffset - offset;
                b += jump;

                // Move onto the next merge region
                ++mergeIt;
            } else if (isDirtyByte && !diffInProgress) {
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

void SnapshotData::addMergeRegion(uint32_t offset,
                                  size_t length,
                                  SnapshotDataType dataType,
                                  SnapshotMergeOperation operation)
{
    // TODO - do we need locking here?
    SnapshotMergeRegion region{ .offset = offset,
                                .length = length,
                                .dataType = dataType,
                                .operation = operation };

    mergeRegions[offset] = region;
}
}
