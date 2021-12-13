#include <faabric/util/locks.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>
#include <faabric/util/memory.h>
#include <faabric/util/snapshot.h>

#include <sys/mman.h>

namespace faabric::util {

SnapshotData::SnapshotData(size_t sizeIn)
  : SnapshotData(sizeIn, sizeIn)
{}

SnapshotData::SnapshotData(size_t sizeIn, size_t maxSizeIn)
  : size(sizeIn)
  , maxSize(maxSizeIn)
  , owner(true)
{
    if (maxSize == 0) {
        maxSize = size;
    }

    // Allocate virtual memory big enough for the max size if provided
    _data = faabric::util::allocateVirtualMemory(maxSize);

    // Claim just the snapshot region
    faabric::util::claimVirtualMemory({ BYTES(_data.get()), size });
}

SnapshotData::SnapshotData(std::vector<uint8_t>& dataIn)
  : SnapshotData({ dataIn.data(), dataIn.size() })
{}

SnapshotData::SnapshotData(std::span<uint8_t> dataIn)
  : SnapshotData(dataIn, dataIn.size())
{}

SnapshotData::SnapshotData(std::span<uint8_t> dataIn, size_t maxSizeIn)
  : size(dataIn.size())
  , maxSize(maxSizeIn)
  , owner(false)
  , _data(wrapNotOwnedRegion(dataIn.data()))
{}

SnapshotData::~SnapshotData()
{
    if (fd > 0) {
        SPDLOG_TRACE("Closing fd {}", fd);
        ::close(fd);
        fd = 0;
    }
}

bool SnapshotData::isRestorable()
{
    faabric::util::SharedLock lock(snapMx);
    return fd > 0;
}

bool SnapshotData::isOwner()
{
    faabric::util::SharedLock lock(snapMx);
    return owner;
}

void SnapshotData::copyInData(std::vector<uint8_t>& buffer, uint32_t offset)
{
    copyInData({ buffer.data(), buffer.size() }, offset);
}

void SnapshotData::copyInData(std::span<uint8_t> buffer, uint32_t offset)
{
    faabric::util::FullLock lock(snapMx);

    // Try to allocate more memory on top of existing data if necessary.
    // Will throw an exception if not possible
    size_t newSize = offset + buffer.size();
    if (newSize > size) {
        if (newSize > maxSize) {
            SPDLOG_ERROR(
              "Copying snapshot data over max: {} > {}", newSize, maxSize);
            throw std::runtime_error("Copying snapshot data over max");
        }

        claimVirtualMemory({ _data.get(), newSize });
        size = newSize;

        // Update fd
        if (fd > 0) {
            // TODO - extend fd
        }
    }

    // Copy in new data
    uint8_t* copyTarget = validateDataOffset(offset);
    ::memcpy(copyTarget, buffer.data(), buffer.size());

    if (fd > 0) {
        // TODO - update fd contents
    }
}

const uint8_t* SnapshotData::getDataPtr(uint32_t offset)
{
    faabric::util::SharedLock lock(snapMx);
    return validateDataOffset(offset);
}

uint8_t* SnapshotData::getMutableDataPtr(uint32_t offset)
{
    faabric::util::SharedLock lock(snapMx);
    return validateDataOffset(offset);
}

uint8_t* SnapshotData::validateDataOffset(uint32_t offset)
{
    if (offset > size) {
        SPDLOG_ERROR("Out of bounds snapshot access: {} > {}", offset, size);
        throw std::runtime_error("Out of bounds snapshot access");
    }

    return _data.get() + offset;
}

std::vector<uint8_t> SnapshotData::getDataCopy()
{
    return getDataCopy(0, size);
}

std::vector<uint8_t> SnapshotData::getDataCopy(uint32_t offset, size_t dataSize)
{
    faabric::util::SharedLock lock(snapMx);

    if (offset + dataSize > size) {
        SPDLOG_ERROR("Out of bounds snapshot access: {} + {} > {}",
                     offset,
                     dataSize,
                     size);
        throw std::runtime_error("Out of bounds snapshot access");
    }

    uint8_t* ptr = validateDataOffset(offset);
    return std::vector<uint8_t>(ptr, ptr + dataSize);
}

std::vector<SnapshotDiff> SnapshotData::getDirtyRegions()
{
    faabric::util::SharedLock lock(snapMx);

    if (_data == nullptr || size == 0) {
        std::vector<SnapshotDiff> empty;
        return empty;
    }

    // Get dirty regions
    int nPages = getRequiredHostPages(size);
    std::vector<int> dirtyPageNumbers =
      getDirtyPageNumbers(_data.get(), nPages);

    std::vector<std::pair<uint32_t, uint32_t>> regions =
      faabric::util::getDirtyRegions(_data.get(), nPages);

    // Convert to snapshot diffs
    std::vector<SnapshotDiff> diffs;
    for (auto& p : regions) {
        diffs.emplace_back(SnapshotDataType::Raw,
                           SnapshotMergeOperation::Overwrite,
                           p.first,
                           _data.get() + p.first,
                           (p.second - p.first));
    }

    SPDLOG_DEBUG("Snapshot has {}/{} dirty pages", diffs.size(), nPages);

    return diffs;
}

std::vector<SnapshotDiff> SnapshotData::getChangeDiffs(
  std::span<const uint8_t> updated)
{
    faabric::util::SharedLock lock(snapMx);

    std::vector<SnapshotDiff> diffs;
    if (mergeRegions.empty()) {
        SPDLOG_DEBUG("No merge regions set, thus no diffs");
        return diffs;
    }

    // Work out which regions of memory have changed
    size_t nThisPages = getRequiredHostPages(updated.size());
    std::vector<std::pair<uint32_t, uint32_t>> dirtyRegions =
      faabric::util::getDirtyRegions(updated.data(), nThisPages);
    SPDLOG_TRACE("Found {} dirty regions", dirtyRegions.size());

    // Iterate through merge regions, see which ones overlap with dirty memory
    // regions, and add corresponding diffs
    for (auto& mrPair : mergeRegions) {
        SnapshotMergeRegion& mr = mrPair.second;

        SPDLOG_TRACE("Merge region {} {} at {}-{}",
                     snapshotDataTypeStr(mr.dataType),
                     snapshotMergeOpStr(mr.operation),
                     mr.offset,
                     mr.offset + mr.length);

        for (auto& dirtyRegion : dirtyRegions) {
            // Add the diffs
            mr.addDiffs(diffs,
                        _data.get(),
                        size,
                        updated.data(),
                        dirtyRegion.first,
                        dirtyRegion.second);
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
    faabric::util::FullLock lock(snapMx);

    SnapshotMergeRegion region{ .offset = offset,
                                .length = length,
                                .dataType = dataType,
                                .operation = operation };

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

void SnapshotData::mapToMemory(uint8_t* target)
{
    faabric::util::FullLock lock(snapMx);

    if (fd == 0) {
        std::string msg = "Attempting to map memory of non-restorable snapshot";
        SPDLOG_ERROR(msg);
        throw std::runtime_error(msg);
    }

    faabric::util::mapMemory({ target, size }, fd);
}

void SnapshotData::makeRestorable(const std::string& fdLabel)
{
    faabric::util::FullLock lock(snapMx);
    fd = writeMemoryToFd({ _data.get(), size }, fdLabel);
}

std::map<uint32_t, SnapshotMergeRegion> SnapshotData::getMergeRegions()
{
    faabric::util::SharedLock lock(snapMx);
    return mergeRegions;
}

void SnapshotData::clearMergeRegions()
{
    faabric::util::FullLock lock(snapMx);
    mergeRegions.clear();
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
                                   uint32_t originalSize,
                                   const uint8_t* updated,
                                   uint32_t dirtyRegionStart,
                                   uint32_t dirtyRegionEnd)
{
    // If the region has zero length, it signifies that it goes to the
    // end of the memory, so we go all the way to the end of the dirty region.
    // For all other regions, we just check if the dirty range is within the
    // merge region.
    bool isInRange = (dirtyRegionEnd > offset) &&
                     ((length == 0) || (dirtyRegionStart < offset + length));

    if (!isInRange) {
        return;
    }

    SPDLOG_TRACE("Checking for {} {} merge region in dirty region {}-{}",
                 snapshotDataTypeStr(dataType),
                 snapshotMergeOpStr(operation),
                 dirtyRegionStart,
                 dirtyRegionEnd);

    switch (dataType) {
        case (SnapshotDataType::Int): {
            // Check if the value has changed
            const uint8_t* updatedValue = updated + offset;
            int updatedInt = *(reinterpret_cast<const int*>(updatedValue));

            if (originalSize < offset) {
                throw std::runtime_error(
                  "Do not support int operations outside original snapshot");
            }

            const uint8_t* originalValue = original + offset;
            int originalInt = *(reinterpret_cast<const int*>(originalValue));

            // Skip if no change
            if (originalInt == updatedInt) {
                return;
            }

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

            // Add the diff
            diffs.emplace_back(
              dataType, operation, offset, updatedValue, length);

            SPDLOG_TRACE("Adding {} {} diff at {}-{} ({})",
                         snapshotDataTypeStr(dataType),
                         snapshotMergeOpStr(operation),
                         offset,
                         offset + length,
                         updatedInt);

            break;
        }
        case (SnapshotDataType::Raw): {
            switch (operation) {
                case (SnapshotMergeOperation::Overwrite): {
                    // Work out bounds of region we're checking
                    uint32_t checkStart =
                      std::max<uint32_t>(dirtyRegionStart, offset);

                    uint32_t checkEnd;
                    if (length == 0) {
                        checkEnd = dirtyRegionEnd;
                    } else {
                        checkEnd =
                          std::min<uint32_t>(dirtyRegionEnd, offset + length);
                    }

                    bool diffInProgress = false;
                    int diffStart = 0;
                    for (int b = checkStart; b <= checkEnd; b++) {
                        // If this byte is outside the original region, we can't
                        // compare (i.e. always dirty)
                        bool isDirtyByte = (b > originalSize) ||
                                           (*(original + b) != *(updated + b));

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
                        int finalDiffLength = checkEnd - diffStart;
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
