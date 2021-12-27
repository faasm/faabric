#include <faabric/util/bytes.h>
#include <faabric/util/gids.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>
#include <faabric/util/memory.h>
#include <faabric/util/snapshot.h>

#include <sys/mman.h>

namespace faabric::util {

SnapshotDiff::SnapshotDiff(SnapshotDataType dataTypeIn,
                           SnapshotMergeOperation operationIn,
                           uint32_t offsetIn,
                           std::span<const uint8_t> dataIn)
  : dataType(dataTypeIn)
  , operation(operationIn)
  , offset(offsetIn)
  , data(dataIn.begin(), dataIn.end())
{}

std::vector<uint8_t> SnapshotDiff::getDataCopy() const
{
    return std::vector<uint8_t>(data.begin(), data.end());
}

SnapshotData::SnapshotData(size_t sizeIn)
  : SnapshotData(sizeIn, sizeIn)
{}

SnapshotData::SnapshotData(size_t sizeIn, size_t maxSizeIn)
  : size(sizeIn)
  , maxSize(maxSizeIn)
{
    if (maxSize == 0) {
        maxSize = size;
    }

    // Allocate virtual memory big enough for the max size if provided
    data = faabric::util::allocateVirtualMemory(maxSize);

    // Claim just the snapshot region
    faabric::util::claimVirtualMemory({ BYTES(data.get()), size });

    // Set up the fd with a two-way mapping to the data
    std::string fdLabel = "snap_" + std::to_string(generateGid());
    fd = createFd(size, fdLabel);
    mapMemoryShared({ data.get(), size }, fd);
}

SnapshotData::SnapshotData(std::span<const uint8_t> dataIn)
  : SnapshotData(dataIn.size())
{
    writeData(dataIn);
}

SnapshotData::SnapshotData(std::span<const uint8_t> dataIn, size_t maxSizeIn)
  : SnapshotData(dataIn.size(), maxSizeIn)
{
    writeData(dataIn);
}

SnapshotData::~SnapshotData()
{
    if (fd > 0) {
        SPDLOG_TRACE("Closing fd {}", fd);
        ::close(fd);
        fd = -1;
    }
}

void SnapshotData::copyInData(std::span<const uint8_t> buffer, uint32_t offset)
{
    faabric::util::FullLock lock(snapMx);

    writeData(buffer, offset);
}

void SnapshotData::writeData(std::span<const uint8_t> buffer, uint32_t offset)
{
    // Try to allocate more memory on top of existing data if necessary.
    // Will throw an exception if not possible
    size_t newSize = offset + buffer.size();
    if (newSize > size) {
        if (newSize > maxSize) {
            SPDLOG_ERROR(
              "Copying snapshot data over max: {} > {}", newSize, maxSize);
            throw std::runtime_error("Copying snapshot data over max");
        }

        claimVirtualMemory({ data.get(), newSize });
        size = newSize;

        // Update fd
        if (fd > 0) {
            resizeFd(fd, newSize);
        }

        // Remap data
        mapMemoryShared({ data.get(), size }, fd);
    }

    // Copy in new data
    uint8_t* copyTarget = validatedOffsetPtr(offset);
    ::memcpy(copyTarget, buffer.data(), buffer.size());
}

const uint8_t* SnapshotData::getDataPtr(uint32_t offset)
{
    faabric::util::SharedLock lock(snapMx);
    return validatedOffsetPtr(offset);
}

uint8_t* SnapshotData::validatedOffsetPtr(uint32_t offset)
{
    if (offset > size) {
        SPDLOG_ERROR("Out of bounds snapshot access: {} > {}", offset, size);
        throw std::runtime_error("Out of bounds snapshot access");
    }

    return data.get() + offset;
}

std::vector<uint8_t> SnapshotData::getDataCopy()
{
    return getDataCopy(0, size);
}

std::vector<uint8_t> SnapshotData::getDataCopy(uint32_t offset, size_t dataSize)
{
    faabric::util::SharedLock lock(snapMx);

    if ((offset + dataSize) > size) {
        SPDLOG_ERROR("Out of bounds snapshot access: {} + {} > {}",
                     offset,
                     dataSize,
                     size);
        throw std::runtime_error("Out of bounds snapshot access");
    }

    uint8_t* ptr = validatedOffsetPtr(offset);
    return std::vector<uint8_t>(ptr, ptr + dataSize);
}

void SnapshotData::addMergeRegion(uint32_t offset,
                                  size_t length,
                                  SnapshotDataType dataType,
                                  SnapshotMergeOperation operation,
                                  bool overwrite)
{
    faabric::util::FullLock lock(snapMx);

    SnapshotMergeRegion region(offset, length, dataType, operation);

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

void SnapshotData::fillGapsWithOverwriteRegions()
{
    faabric::util::FullLock lock(snapMx);

    // If there's no merge regions, just do one big one (note, zero length means
    // fill all space
    if (mergeRegions.empty()) {
        SPDLOG_TRACE("Filling gap with single overwrite merge region");
        mergeRegions.emplace(std::pair<uint32_t, SnapshotMergeRegion>(
          0,
          { 0, 0, SnapshotDataType::Raw, SnapshotMergeOperation::Overwrite }));

        return;
    }

    uint32_t lastRegionEnd = 0;
    for (auto [offset, region] : mergeRegions) {
        if (offset == 0) {
            // Zeroth byte is in a merge region
            lastRegionEnd = region.length;
            continue;
        }

        uint32_t regionLen = region.offset - lastRegionEnd;

        SPDLOG_TRACE("Filling gap with overwrite merge region {}-{}",
                     lastRegionEnd,
                     lastRegionEnd + regionLen);

        mergeRegions.emplace(std::pair<uint32_t, SnapshotMergeRegion>(
          lastRegionEnd,
          { lastRegionEnd,
            regionLen,
            SnapshotDataType::Raw,
            SnapshotMergeOperation::Overwrite }));

        lastRegionEnd = region.offset + region.length;
    }

    if (lastRegionEnd < size) {
        // Add a final region at the end of the snapshot
        mergeRegions.emplace(std::pair<uint32_t, SnapshotMergeRegion>(
          lastRegionEnd,
          { lastRegionEnd,
            0,
            SnapshotDataType::Raw,
            SnapshotMergeOperation::Overwrite }));
    }
}

void SnapshotData::mapToMemory(uint8_t* target)
{
    faabric::util::FullLock lock(snapMx);

    if (fd <= 0) {
        std::string msg = "Attempting to map memory of non-restorable snapshot";
        SPDLOG_ERROR(msg);
        throw std::runtime_error(msg);
    }

    mapMemoryPrivate({ target, size }, fd);
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

size_t SnapshotData::getQueuedDiffsCount()
{
    faabric::util::SharedLock lock(snapMx);
    return queuedDiffs.size();
}

void SnapshotData::queueDiffs(const std::span<SnapshotDiff> diffs)
{
    faabric::util::FullLock lock(snapMx);
    for (const auto& diff : diffs) {
        queuedDiffs.emplace_back(std::move(diff));
    }
}

void SnapshotData::writeQueuedDiffs()
{
    faabric::util::FullLock lock(snapMx);

    SPDLOG_DEBUG("Writing {} queued diffs to snapshot", queuedDiffs.size());

    // Iterate through diffs
    for (auto& diff : queuedDiffs) {
        if (diff.getOperation() ==
            faabric::util::SnapshotMergeOperation::Ignore) {

            SPDLOG_TRACE("Ignoring region {}-{}",
                         diff.getOffset(),
                         diff.getOffset() + diff.getData().size());

            continue;
        }
        if (diff.getOperation() ==
            faabric::util::SnapshotMergeOperation::Overwrite) {

            SPDLOG_TRACE("Copying overwrite diff into {}-{}",
                         diff.getOffset(),
                         diff.getOffset() + diff.getData().size());

            writeData(diff.getData(), diff.getOffset());

            continue;
        }

        uint8_t* copyTarget = validatedOffsetPtr(diff.getOffset());
        switch (diff.getDataType()) {
            case (faabric::util::SnapshotDataType::Int): {
                int32_t finalValue =
                  applyDiffValue<int32_t>(validatedOffsetPtr(diff.getOffset()),
                                          diff.getData().data(),
                                          diff.getOperation());

                SPDLOG_TRACE("Writing int {} diff: {} {} -> {}",
                             snapshotMergeOpStr(diff.getOperation()),
                             unalignedRead<int32_t>(copyTarget),
                             unalignedRead<int32_t>(diff.getData().data()),
                             finalValue);

                writeData({ BYTES(&finalValue), sizeof(int32_t) },
                          diff.getOffset());
                break;
            }
            case (faabric::util::SnapshotDataType::Long): {
                long finalValue =
                  applyDiffValue<long>(validatedOffsetPtr(diff.getOffset()),
                                       diff.getData().data(),
                                       diff.getOperation());

                SPDLOG_TRACE("Writing long {} diff: {} {} -> {}",
                             snapshotMergeOpStr(diff.getOperation()),
                             unalignedRead<long>(copyTarget),
                             unalignedRead<long>(diff.getData().data()),
                             finalValue);

                writeData({ BYTES(&finalValue), sizeof(long) },
                          diff.getOffset());
                break;
            }
            case (faabric::util::SnapshotDataType::Float): {
                float finalValue =
                  applyDiffValue<float>(validatedOffsetPtr(diff.getOffset()),
                                        diff.getData().data(),
                                        diff.getOperation());

                SPDLOG_TRACE("Writing float {} diff: {} {} -> {}",
                             snapshotMergeOpStr(diff.getOperation()),
                             unalignedRead<float>(copyTarget),
                             unalignedRead<float>(diff.getData().data()),
                             finalValue);

                writeData({ BYTES(&finalValue), sizeof(float) },
                          diff.getOffset());
                break;
            }
            case (faabric::util::SnapshotDataType::Double): {
                double finalValue =
                  applyDiffValue<double>(validatedOffsetPtr(diff.getOffset()),
                                         diff.getData().data(),
                                         diff.getOperation());

                SPDLOG_TRACE("Writing double {} diff: {} {} -> {}",
                             snapshotMergeOpStr(diff.getOperation()),
                             unalignedRead<double>(copyTarget),
                             unalignedRead<double>(diff.getData().data()),
                             finalValue);

                writeData({ BYTES(&finalValue), sizeof(double) },
                          diff.getOffset());
                break;
            }
            default: {
                SPDLOG_ERROR("Unsupported data type: {}", diff.getDataType());
                throw std::runtime_error("Unsupported merge data type");
            }
        }
    }

    // Clear queue
    queuedDiffs.clear();
}

MemoryView::MemoryView(std::span<const uint8_t> dataIn)
  : data(dataIn)
{}

std::vector<SnapshotDiff> MemoryView::getDirtyRegions()
{
    if (data.empty()) {
        return {};
    }

    // Get dirty regions
    int nPages = getRequiredHostPages(data.size());
    std::vector<int> dirtyPageNumbers =
      getDirtyPageNumbers(data.data(), nPages);

    SPDLOG_DEBUG(
      "Memory view has {}/{} dirty pages", dirtyPageNumbers.size(), nPages);

    std::vector<std::pair<uint32_t, uint32_t>> regions =
      faabric::util::getDirtyRegions(data.data(), nPages);

    // Convert to snapshot diffs
    std::vector<SnapshotDiff> diffs;
    diffs.reserve(regions.size());
    for (auto [regionBegin, regionEnd] : regions) {
        SPDLOG_TRACE("Memory view dirty {}-{}", regionBegin, regionEnd);
        diffs.emplace_back(SnapshotDataType::Raw,
                           SnapshotMergeOperation::Overwrite,
                           regionBegin,
                           data.subspan(regionBegin, regionEnd - regionBegin));
    }

    return diffs;
}

std::vector<SnapshotDiff> MemoryView::diffWithSnapshot(
  std::shared_ptr<SnapshotData> snap)
{
    std::vector<SnapshotDiff> diffs;
    std::map<uint32_t, SnapshotMergeRegion> mergeRegions =
      snap->getMergeRegions();
    if (mergeRegions.empty()) {
        SPDLOG_DEBUG("No merge regions set, thus no diffs");
        return diffs;
    }

    // Work out which regions of memory have changed
    size_t nThisPages = getRequiredHostPages(data.size());
    std::vector<std::pair<uint32_t, uint32_t>> dirtyRegions =
      faabric::util::getDirtyRegions(data.data(), nThisPages);
    SPDLOG_TRACE("Found {} dirty regions at {} over {} pages",
                 dirtyRegions.size(),
                 (void*)data.data(),
                 nThisPages);

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
                        { snap->getDataPtr(), snap->getSize() },
                        data,
                        dirtyRegion);
        }
    }

    return diffs;
}

std::string snapshotDataTypeStr(SnapshotDataType dt)
{
    switch (dt) {
        case (SnapshotDataType::Raw): {
            return "Raw";
        }
        case (SnapshotDataType::Bool): {
            return "Bool";
        }
        case (SnapshotDataType::Int): {
            return "Int";
        }
        case (SnapshotDataType::Long): {
            return "Long";
        }
        case (SnapshotDataType::Float): {
            return "Float";
        }
        case (SnapshotDataType::Double): {
            return "Double";
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

void SnapshotMergeRegion::addOverwriteDiff(
  std::vector<SnapshotDiff>& diffs,
  std::span<const uint8_t> original,
  std::span<const uint8_t> updated,
  std::pair<uint32_t, uint32_t> dirtyRange)
{
    auto operation = SnapshotMergeOperation::Overwrite;

    // Work out bounds of region we're checking
    uint32_t checkStart = std::max<uint32_t>(dirtyRange.first, offset);

    // Here we need to make sure we don't overrun the original or the updated
    // data
    uint32_t checkEnd;
    if (length == 0) {
        checkEnd = dirtyRange.second;
    } else {
        checkEnd = std::min<uint32_t>(dirtyRange.second, offset + length);
    }

    bool diffInProgress = false;
    int diffStart = 0;
    for (int b = checkStart; b <= checkEnd; b++) {
        // If this byte is outside the original region, we can't
        // compare (i.e. always dirty)
        bool isDirtyByte = (b > original.size()) ||
                           (*(original.data() + b) != *(updated.data() + b));

        if (isDirtyByte && !diffInProgress) {
            // Diff starts here if it's different and diff
            // not in progress
            diffInProgress = true;
            diffStart = b;
        } else if (!isDirtyByte && diffInProgress) {
            // Diff ends if it's not different and diff is
            // in progress
            int diffLength = b - diffStart;
            SPDLOG_TRACE("Found {} overwrite diff at {}-{}",
                         snapshotDataTypeStr(dataType),
                         diffStart,
                         diffStart + diffLength);

            diffInProgress = false;
            diffs.emplace_back(dataType,
                               operation,
                               diffStart,
                               updated.subspan(diffStart, diffLength));
        }
    }

    // If we've reached the end of this region with a diff
    // in progress, we need to close it off
    if (diffInProgress) {
        int finalDiffLength = checkEnd - diffStart;
        SPDLOG_TRACE("Adding {} {} diff at {}-{} (end of region)",
                     snapshotDataTypeStr(dataType),
                     snapshotMergeOpStr(operation),
                     diffStart,
                     diffStart + finalDiffLength);

        diffs.emplace_back(dataType,
                           operation,
                           diffStart,
                           updated.subspan(diffStart, finalDiffLength));
    }
}

SnapshotMergeRegion::SnapshotMergeRegion(uint32_t offsetIn,
                                         size_t lengthIn,
                                         SnapshotDataType dataTypeIn,
                                         SnapshotMergeOperation operationIn)
  : offset(offsetIn)
  , length(lengthIn)
  , dataType(dataTypeIn)
  , operation(operationIn)
{}

void SnapshotMergeRegion::addDiffs(std::vector<SnapshotDiff>& diffs,
                                   std::span<const uint8_t> originalData,
                                   std::span<const uint8_t> updatedData,
                                   std::pair<uint32_t, uint32_t> dirtyRange)
{
    // If the region has zero length, it signifies that it goes to the
    // end of the memory, so we go all the way to the end of the dirty region.
    // For all other regions, we just check if the dirty range is within the
    // merge region.
    bool isInRange = (dirtyRange.second > offset) &&
                     ((length == 0) || (dirtyRange.first < offset + length));

    if (!isInRange) {
        SPDLOG_TRACE("{} {} merge region {}-{} not in dirty region {}-{}",
                     snapshotDataTypeStr(dataType),
                     snapshotMergeOpStr(operation),
                     offset,
                     offset + length,
                     dirtyRange.first,
                     dirtyRange.second);
        return;
    }

    SPDLOG_TRACE("{} {} merge region {}-{} aligns with dirty region {}-{}",
                 snapshotDataTypeStr(dataType),
                 snapshotMergeOpStr(operation),
                 offset,
                 offset + length,
                 dirtyRange.first,
                 dirtyRange.second);

    if (operation == SnapshotMergeOperation::Overwrite) {
        addOverwriteDiff(diffs, originalData, updatedData, dirtyRange);
        return;
    }

    if (operation == SnapshotMergeOperation::Ignore) {
        return;
    }

    if (originalData.size() < offset) {
        throw std::runtime_error(
          "Do not support non-overwrite operations outside original snapshot");
    }

    uint8_t* updated = (uint8_t*)updatedData.data() + offset;
    const uint8_t* original = originalData.data() + offset;

    bool changed = false;
    switch (dataType) {
        case (SnapshotDataType::Int): {
            int preUpdate = unalignedRead<int>(updated);
            changed = calculateDiffValue<int>(original, updated, operation);

            SPDLOG_TRACE("Calculated int {} merge: {} {} -> {}",
                         snapshotMergeOpStr(operation),
                         preUpdate,
                         unalignedRead<int>(original),
                         unalignedRead<int>(updated));
            break;
        }
        case (SnapshotDataType::Long): {
            long preUpdate = unalignedRead<long>(updated);
            changed = calculateDiffValue<long>(original, updated, operation);

            SPDLOG_TRACE("Calculated long {} merge: {} {} -> {}",
                         snapshotMergeOpStr(operation),
                         preUpdate,
                         unalignedRead<long>(original),
                         unalignedRead<long>(updated));
            break;
        }
        case (SnapshotDataType::Float): {
            float preUpdate = unalignedRead<float>(updated);
            changed = calculateDiffValue<float>(original, updated, operation);

            SPDLOG_TRACE("Calculated float {} merge: {} {} -> {}",
                         snapshotMergeOpStr(operation),
                         preUpdate,
                         unalignedRead<float>(original),
                         unalignedRead<float>(updated));
            break;
        }
        case (SnapshotDataType::Double): {
            double preUpdate = unalignedRead<double>(updated);
            changed = calculateDiffValue<double>(original, updated, operation);

            SPDLOG_TRACE("Calculated double {} merge: {} {} -> {}",
                         snapshotMergeOpStr(operation),
                         preUpdate,
                         unalignedRead<double>(original),
                         unalignedRead<double>(updated));
            break;
        }
        default: {
            SPDLOG_ERROR("Unsupported merge op combination {} {}",
                         snapshotDataTypeStr(dataType),
                         snapshotMergeOpStr(operation));
            throw std::runtime_error("Unsupported merge op combination");
        }
    }

    // Add the diff
    if (changed) {
        diffs.emplace_back(dataType,
                           operation,
                           offset,
                           std::span<const uint8_t>(updated, length));
    }
}
}
