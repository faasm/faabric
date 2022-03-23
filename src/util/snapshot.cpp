#include <faabric/util/bytes.h>
#include <faabric/util/dirty.h>
#include <faabric/util/gids.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>
#include <faabric/util/memory.h>
#include <faabric/util/snapshot.h>
#include <faabric/util/timing.h>

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

void diffArrayRegions(std::vector<SnapshotDiff>& snapshotDiffs,
                      uint32_t startOffset,
                      uint32_t endOffset,
                      std::span<const uint8_t> a,
                      std::span<const uint8_t> b)
{
    // Iterate through diffs and work out start and finish offsets of each dirty
    // region
    uint32_t diffStart = 0;
    bool diffInProgress = false;
    for (uint32_t i = startOffset; i < endOffset; i++) {
        bool dirty = a.data()[i] != b.data()[i];
        if (dirty && !diffInProgress) {
            // Starts at this byte
            diffInProgress = true;
            diffStart = i;
        } else if (!dirty && diffInProgress) {
            // Finished on byte before
            diffInProgress = false;
            snapshotDiffs.emplace_back(SnapshotDataType::Raw,
                                       SnapshotMergeOperation::Bytewise,
                                       diffStart,
                                       b.subspan(diffStart, i - diffStart));
        }
    }

    // If we finish with a diff in progress, add it
    if (diffInProgress) {
        snapshotDiffs.emplace_back(SnapshotDataType::Raw,
                                   SnapshotMergeOperation::Bytewise,
                                   diffStart,
                                   b.subspan(diffStart, endOffset - diffStart));
    }
}

SnapshotData::SnapshotData()
  : SnapshotData(0, 0)
{}

SnapshotData::SnapshotData(size_t sizeIn)
  : SnapshotData(sizeIn, sizeIn)
{}

SnapshotData::SnapshotData(size_t sizeIn, size_t maxSizeIn)
  : conf(faabric::util::getSystemConfig())
  , size(sizeIn)
  , maxSize(maxSizeIn)
  , isDemandPaged(conf.dirtyTrackingMode == "uffd-demand" ||
                  conf.dirtyTrackingMode == "uffd-thread-demand")
{
    if (maxSize == 0) {
        maxSize = size;
    }

    // Allocate virtual memory big enough for the max size if provided
    data = faabric::util::allocateVirtualMemory(maxSize);

    // Claim just the snapshot region
    faabric::util::claimVirtualMemory({ BYTES(data.get()), size });

    // Set up the fd with a two-way mapping to the data if necessary
    if (!isDemandPaged) {
        std::string fdLabel = "snap_" + std::to_string(generateGid());
        fd = createFd(size, fdLabel);
        mapMemoryShared({ data.get(), size }, fd);
    }
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

void SnapshotData::checkWriteExtension(std::span<const uint8_t> buffer,
                                       uint32_t offset)
{
    // Try to allocate more memory on top of existing data if necessary.
    // Will throw an exception if not possible
    size_t regionEnd = offset + buffer.size();
    if (regionEnd > size) {
        if (regionEnd > maxSize) {
            SPDLOG_ERROR(
              "Copying snapshot data over max: {} > {}", regionEnd, maxSize);
            throw std::runtime_error("Copying snapshot data over max");
        }

        claimVirtualMemory({ data.get(), regionEnd });
        size = regionEnd;

        // Update fd
        if (fd > 0) {
            resizeFd(fd, regionEnd);
        }

        // Remap data
        if (!isDemandPaged) {
            mapMemoryShared({ data.get(), size }, fd);
        }
    }
}

void SnapshotData::writeData(std::span<const uint8_t> buffer, uint32_t offset)
{
    size_t regionEnd = offset + buffer.size();
    checkWriteExtension(buffer, offset);

    // Copy in new data
    uint8_t* copyTarget = validatedOffsetPtr(offset);
    ::memcpy(copyTarget, buffer.data(), buffer.size());

    // Record the change
    trackedChanges.emplace_back(offset, regionEnd);
}

void SnapshotData::xorData(std::span<const uint8_t> buffer, uint32_t offset)
{
    size_t regionEnd = offset + buffer.size();
    if (regionEnd > size) {
        SPDLOG_ERROR(
          "XORing snapshot data exceeding size: {} > {}", regionEnd, size);
        throw std::runtime_error("XORing data exceeding size");
    }

    uint8_t* copyTarget = validatedOffsetPtr(offset);
    std::transform(
      buffer.begin(), buffer.end(), copyTarget, copyTarget, std::bit_xor());

    trackedChanges.emplace_back(offset, regionEnd);
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
                                  SnapshotMergeOperation operation)
{
    faabric::util::FullLock lock(snapMx);

    SPDLOG_DEBUG("Adding new {} {} merge region at {}-{}",
                 snapshotDataTypeStr(dataType),
                 snapshotMergeOpStr(operation),
                 offset,
                 offset + length);

    mergeRegions.emplace_back(offset, length, dataType, operation);
}

void SnapshotData::fillGapsWithBytewiseRegions()
{
    faabric::util::FullLock lock(snapMx);

    SnapshotMergeOperation fillType;
    if (conf.diffingMode == "bytewise") {
        fillType = SnapshotMergeOperation::Bytewise;
    } else if (conf.diffingMode == "xor") {
        fillType = SnapshotMergeOperation::XOR;
    } else {
        SPDLOG_ERROR("Unsupported diffing mode: {}", conf.diffingMode);
        throw std::runtime_error("Unsupported diffing mode");
    }

    // If there's no merge regions, just do one big one (note, zero length means
    // fill all space
    if (mergeRegions.empty()) {
        SPDLOG_TRACE("Filling gap with single bytewise merge region");
        mergeRegions.emplace_back(0, 0, SnapshotDataType::Raw, fillType);

        return;
    }

    // We're modifying the regions within the loop so need to make a copy
    std::vector<SnapshotMergeRegion> regionsCopy = mergeRegions;

    // Sort merge regions to ensure loop below works
    std::sort(regionsCopy.begin(), regionsCopy.end());

    // Iterate through the merge regions, adding regions that fill the gaps
    // between them
    uint32_t lastRegionEnd = 0;
    for (const auto& r : regionsCopy) {
        // This checks whether the very first byte is in a merge region
        if (r.offset == 0) {
            lastRegionEnd = r.length;
            continue;
        }

        // This checks whether we have two adjacent regions
        if (r.offset == lastRegionEnd) {
            lastRegionEnd += r.length;
            continue;
        }

        uint32_t regionLen = r.offset - lastRegionEnd;
        SPDLOG_TRACE("Filling gap with bytewise merge region {}-{}",
                     lastRegionEnd,
                     lastRegionEnd + regionLen);

        mergeRegions.emplace_back(
          lastRegionEnd, regionLen, SnapshotDataType::Raw, fillType);

        lastRegionEnd = r.offset + r.length;
    }

    if (lastRegionEnd < size) {
        SPDLOG_TRACE("Filling final gap with merge region starting at {}",
                     lastRegionEnd);

        // Add a final region at the end of the snapshot
        mergeRegions.emplace_back(
          lastRegionEnd, 0, SnapshotDataType::Raw, fillType);
    }
}

void SnapshotData::mapToMemory(std::span<uint8_t> target)
{
    // Note we only need a shared lock here as we are not modifying data and the
    // OS will handle synchronisation of the mapping itself
    PROF_START(MapSnapshot)
    faabric::util::SharedLock lock(snapMx);

    if (target.size() > size) {
        SPDLOG_ERROR("Mapping target memory larger than snapshot ({} > {})",
                     target.size(),
                     size);
        throw std::runtime_error("Target memory larger than snapshot");
    }

    // Set up mapping
    if (isDemandPaged) {
        auto tracker = faabric::util::getDirtyTracker();
        tracker->mapRegions(std::span<uint8_t>(data.get(), size), target);
    } else {
        faabric::util::mapMemoryPrivate(target, fd);
    }

    // Warn kernel we will likely need this memory
    ::madvise(target.data(), size, MADV_WILLNEED);

    // Advise huge pages
    ::madvise(target.data(), size, MADV_HUGEPAGE);

    PROF_END(MapSnapshot)
}

std::vector<SnapshotMergeRegion> SnapshotData::getMergeRegions()
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

void SnapshotData::queueDiffs(const std::vector<SnapshotDiff>& diffs)
{
    if (diffs.empty()) {
        return;
    }

    faabric::util::FullLock lock(snapMx);
    for (const auto& diff : diffs) {
        queuedDiffs.emplace_back(std::move(diff));
    }
}

int SnapshotData::writeQueuedDiffs()
{
    PROF_START(WriteQueuedDiffs)
    faabric::util::FullLock lock(snapMx);

    SPDLOG_DEBUG("Writing {} queued diffs to snapshot", queuedDiffs.size());

    // Iterate through diffs
    int nDiffs = queuedDiffs.size();
    for (auto& diff : queuedDiffs) {
        if (diff.getOperation() ==
            faabric::util::SnapshotMergeOperation::Ignore) {

            SPDLOG_TRACE("Ignoring region {}-{}",
                         diff.getOffset(),
                         diff.getOffset() + diff.getData().size());

            continue;
        }

        if (diff.getOperation() ==
            faabric::util::SnapshotMergeOperation::Bytewise) {
            writeData(diff.getData(), diff.getOffset());
            continue;
        }

        if (diff.getOperation() == faabric::util::SnapshotMergeOperation::XOR) {
            xorData(diff.getData(), diff.getOffset());
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
    PROF_END(WriteQueuedDiffs)

    return nDiffs;
}

void SnapshotData::clearTrackedChanges()
{
    faabric::util::FullLock lock(snapMx);
    trackedChanges.clear();
}

std::vector<faabric::util::SnapshotDiff> SnapshotData::getTrackedChanges()
{
    faabric::util::SharedLock lock(snapMx);

    std::vector<SnapshotDiff> diffs;
    if (trackedChanges.empty()) {
        return diffs;
    }

    std::span<uint8_t> d(data.get(), size);

    // Convert to snapshot diffs
    diffs.reserve(trackedChanges.size());
    for (auto [regionBegin, regionEnd] : trackedChanges) {
        SPDLOG_TRACE("Snapshot dirty {}-{}", regionBegin, regionEnd);
        diffs.emplace_back(SnapshotDataType::Raw,
                           SnapshotMergeOperation::Bytewise,
                           regionBegin,
                           d.subspan(regionBegin, regionEnd - regionBegin));
    }

    return diffs;
}

std::vector<faabric::util::SnapshotDiff> SnapshotData::diffWithDirtyRegions(
  std::span<uint8_t> updated,
  const std::vector<char>& dirtyRegions)
{
    faabric::util::SharedLock lock(snapMx);

    PROF_START(DiffWithSnapshot)
    std::vector<faabric::util::SnapshotDiff> diffs;

    // Always add a bytewise region that covers any extension of the
    // updated data
    if (updated.size() > size) {
        PROF_START(ExtensionDiff)
        SPDLOG_TRACE(
          "Adding diff to extend snapshot from {} to {}", size, updated.size());
        size_t extensionLen = updated.size() - size;
        diffs.emplace_back(SnapshotDataType::Raw,
                           SnapshotMergeOperation::Bytewise,
                           size,
                           updated.subspan(size, extensionLen));
        PROF_END(ExtensionDiff)
    }

    // Check to see if we can skip with no dirty regions
    PROF_START(DiffDirtySkip)
    if (dirtyRegions.empty() ||
        (std::find(dirtyRegions.begin(), dirtyRegions.end(), 1) ==
         dirtyRegions.end())) {
        SPDLOG_TRACE("No dirty pages, no diffs");
        return diffs;
    }
    PROF_END(DiffDirtySkip)

    // Check to see if we can skip with no merge regions
    if (mergeRegions.empty()) {
        SPDLOG_TRACE("No merge regions, no diffs");
        return diffs;
    }

    // Sort merge regions. This is not strictly necessary but makes testing and
    // debugging a lot easier and doesn't take long. Could be removed if it
    // became a bottleneck.
    std::sort(mergeRegions.begin(), mergeRegions.end());

    // Iterate through merge regions, allow them to add diffs based on the
    // dirty regions
    std::span<const uint8_t> original(data.get(), size);
    std::span<uint8_t> updatedOverlap = updated.subspan(0, size);
    for (auto& mr : mergeRegions) {
        mr.addDiffs(diffs, original, updatedOverlap, dirtyRegions);
    }

    PROF_END(DiffWithSnapshot)
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
        case (SnapshotMergeOperation::Bytewise): {
            return "Bytewise";
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
        case (SnapshotMergeOperation::XOR): {
            return "XOR";
        }
        default: {
            SPDLOG_ERROR("Cannot convert snapshot merge op to string: {}", op);
            throw std::runtime_error("Cannot convert merge op to string");
        }
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
                                   std::span<uint8_t> updatedData,
                                   const std::vector<char>& dirtyRegions)
{
    if (operation == SnapshotMergeOperation::Ignore) {
        return;
    }

    // If the region is past the end of the original data, we ignore it
    if (offset > originalData.size()) {
        SPDLOG_TRACE("Ignoring {} {} merge {}-{} past end of original at {}",
                     snapshotDataTypeStr(dataType),
                     snapshotMergeOpStr(operation),
                     offset,
                     offset + length,
                     originalData.size());
        return;
    }

    // If the region has zero length, we go all the way to the end of the
    // original data. We also make sure we don't go over the end of the
    // original
    uint32_t mrEnd = length > 0 ? offset + length : originalData.size();
    mrEnd = std::min<uint32_t>(mrEnd, originalData.size());

    // Note that this range will be exclusive, i.e. endPage is the page we
    // stop at the start of
    size_t startPage = getRequiredHostPagesRoundDown(offset);
    size_t endPage = getRequiredHostPages(mrEnd);

    SPDLOG_TRACE("Checking {} {} merge {}-{} over pages {}-{}",
                 snapshotDataTypeStr(dataType),
                 snapshotMergeOpStr(operation),
                 offset,
                 offset + length,
                 startPage,
                 endPage - 1);

    // Check if anything dirty in the given region (std::find range is
    // exclusive)
    auto startIt = dirtyRegions.begin() + startPage;
    auto endIt = dirtyRegions.begin() + endPage;
    auto foundIt = std::find(startIt, endIt, 1);
    if (foundIt == endIt) {
        SPDLOG_TRACE("No dirty pages for {} {} {}-{} ({})",
                     snapshotDataTypeStr(dataType),
                     snapshotMergeOpStr(operation),
                     offset,
                     offset + length,
                     mrEnd);
        return;
    }
    startPage += std::distance(startIt, foundIt);

    // Bytewise and XOR both deal with overwriting bytes without any
    // other logic. Bytewise will filter in only the modified bytes,
    // whereas XOR will transmit the XOR of the whole page and the original
    if (operation == SnapshotMergeOperation::Bytewise ||
        operation == SnapshotMergeOperation::XOR) {
        // Iterate through pages
        for (int p = startPage; p < endPage; p++) {
            // Skip if page not dirty
            if (dirtyRegions.at(p) == 0) {
                continue;
            }

            // Stop at merge region boundaries, making sure we don't start
            // checking before the merge region offset, or go over the merge
            // region end on the final page (may not be page-aligned)
            uint32_t startByte = std::max<uint32_t>(offset, p * HOST_PAGE_SIZE);
            uint32_t endByte =
              std::min<uint32_t>(mrEnd, (p + 1) * HOST_PAGE_SIZE);

            SPDLOG_TRACE("Checking page {} {}-{}", p, startByte, endByte);

            if (operation == SnapshotMergeOperation::Bytewise) {
                diffArrayRegions(
                  diffs, startByte, endByte, originalData, updatedData);
            } else {
                uint32_t rangeSize = endByte - startByte;
                std::transform(originalData.begin() + startByte,
                               originalData.begin() + startByte + rangeSize,
                               updatedData.begin() + startByte,
                               updatedData.begin() + startByte,
                               std::bit_xor<uint8_t>());

                SPDLOG_TRACE("Adding {} XOR merge: {}-{}",
                             snapshotDataTypeStr(dataType),
                             startByte,
                             startByte + rangeSize);

                diffs.emplace_back(dataType,
                                   operation,
                                   startByte,
                                   updatedData.subspan(startByte, rangeSize));
            }
        }

        // This is the end of the XOR/bytewise diff
        return;
    }

    uint8_t* updated = updatedData.data() + offset;
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
