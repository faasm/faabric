#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <span>
#include <string>
#include <vector>

#include <faabric/util/dirty.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>
#include <faabric/util/memory.h>

namespace faabric::util {

enum SnapshotDataType
{
    Raw,
    Bool,
    Int,
    Long,
    Float,
    Double
};

enum SnapshotMergeOperation
{
    Overwrite,
    Sum,
    Product,
    Subtract,
    Max,
    Min,
    Ignore
};

class SnapshotDiff
{
  public:
    SnapshotDiff() = default;

    SnapshotDiff(SnapshotDataType dataTypeIn,
                 SnapshotMergeOperation operationIn,
                 uint32_t offsetIn,
                 std::span<const uint8_t> dataIn);

    SnapshotDataType getDataType() const { return dataType; }

    SnapshotMergeOperation getOperation() const { return operation; }

    uint32_t getOffset() const { return offset; }

    std::span<const uint8_t> getData() const { return data; }

    std::vector<uint8_t> getDataCopy() const;

  private:
    SnapshotDataType dataType = SnapshotDataType::Raw;
    SnapshotMergeOperation operation = SnapshotMergeOperation::Overwrite;
    uint32_t offset = 0;
    std::vector<uint8_t> data;
};

class SnapshotMergeRegion
{
  public:
    uint32_t offset = 0;
    size_t length = 0;
    SnapshotDataType dataType = SnapshotDataType::Raw;
    SnapshotMergeOperation operation = SnapshotMergeOperation::Overwrite;

    SnapshotMergeRegion() = default;

    SnapshotMergeRegion(uint32_t offsetIn,
                        size_t lengthIn,
                        SnapshotDataType dataTypeIn,
                        SnapshotMergeOperation operationIn);

    void addDiffs(std::vector<SnapshotDiff>& diffs,
                  std::span<const uint8_t> originalData,
                  OffsetMemoryRegion dirtyRegion);

  private:
    void addOverwriteDiff(std::vector<SnapshotDiff>& diffs,
                          std::span<const uint8_t> original,
                          OffsetMemoryRegion dirtyRegion);
};

template<typename T>
inline bool calculateDiffValue(const uint8_t* original,
                               uint8_t* updated,
                               SnapshotMergeOperation operation)
{
    // Cast to value
    T updatedValue = unalignedRead<T>(updated);
    T originalValue = unalignedRead<T>(original);

    // Skip if no change
    if (originalValue == updatedValue) {
        return false;
    }

    // Work out final result
    switch (operation) {
        case (SnapshotMergeOperation::Sum): {
            // Sums must send the value to be _added_, and
            // not the final result
            updatedValue -= originalValue;
            break;
        }
        case (SnapshotMergeOperation::Subtract): {
            // Subtractions must send the value to be
            // subtracted, not the result
            updatedValue = originalValue - updatedValue;
            break;
        }
        case (SnapshotMergeOperation::Product): {
            // Products must send the value to be
            // multiplied, not the result
            updatedValue /= originalValue;
            break;
        }
        case (SnapshotMergeOperation::Max):
        case (SnapshotMergeOperation::Min):
            // Min and max don't need to change
            break;
        default: {
            SPDLOG_ERROR("Can't calculate diff for operation: {}", operation);
            throw std::runtime_error("Can't calculate diff");
        }
    }

    unalignedWrite<T>(updatedValue, updated);

    return true;
}

template<typename T>
inline T applyDiffValue(const uint8_t* original,
                        const uint8_t* diff,
                        SnapshotMergeOperation operation)
{

    auto diffValue = unalignedRead<T>(diff);
    T originalValue = unalignedRead<T>(original);

    switch (operation) {
        case (SnapshotMergeOperation::Sum): {
            return diffValue + originalValue;
        }
        case (SnapshotMergeOperation::Subtract): {
            return originalValue - diffValue;
        }
        case (SnapshotMergeOperation::Product): {
            return originalValue * diffValue;
        }
        case (SnapshotMergeOperation::Max): {
            return std::max<T>(originalValue, diffValue);
        }
        case (SnapshotMergeOperation::Min): {
            return std::min<T>(originalValue, diffValue);
        }
        default: {
            SPDLOG_ERROR("Can't apply merge operation: {}", operation);
            throw std::runtime_error("Can't apply merge operation");
        }
    }
}

class SnapshotData
{
  public:
    SnapshotData() = default;

    explicit SnapshotData(size_t sizeIn);

    SnapshotData(size_t sizeIn, size_t maxSizeIn);

    explicit SnapshotData(std::span<const uint8_t> dataIn);

    SnapshotData(std::span<const uint8_t> dataIn, size_t maxSizeIn);

    SnapshotData(const SnapshotData&) = delete;

    SnapshotData& operator=(const SnapshotData&) = delete;

    ~SnapshotData();

    void copyInData(std::span<const uint8_t> buffer, uint32_t offset = 0);

    const uint8_t* getDataPtr(uint32_t offset = 0);

    std::vector<uint8_t> getDataCopy();

    std::vector<uint8_t> getDataCopy(uint32_t offset, size_t dataSize);

    void mapToMemory(std::span<uint8_t> target);

    void addMergeRegion(uint32_t offset,
                        size_t length,
                        SnapshotDataType dataType,
                        SnapshotMergeOperation operation,
                        bool overwrite = false);

    void fillGapsWithOverwriteRegions();

    void clearMergeRegions();

    std::map<uint32_t, SnapshotMergeRegion> getMergeRegions();

    size_t getQueuedDiffsCount();

    std::vector<SnapshotDiff> getQueuedDiffs();

    void queueDiffs(std::span<SnapshotDiff> diffs);

    void writeQueuedDiffs();

    size_t getSize() const { return size; }

    size_t getMaxSize() const { return maxSize; }

    void clearTrackedChanges();

    std::vector<SnapshotDiff> getTrackedChanges();

    std::vector<faabric::util::SnapshotDiff> diffWithMemory(
      std::vector<OffsetMemoryRegion> dirtyRegions);

  private:
    size_t size = 0;
    size_t maxSize = 0;

    int fd = -1;

    std::shared_mutex snapMx;

    MemoryRegion data = nullptr;

    std::vector<OffsetMemoryRegion> queuedDirtyRegions;

    std::vector<SnapshotDiff> queuedDiffs;

    std::vector<std::pair<uint32_t, uint32_t>> trackedChanges;

    // Note - we care about the order of this map, as we iterate through it
    // in order of offsets
    std::map<uint32_t, SnapshotMergeRegion> mergeRegions;

    uint8_t* validatedOffsetPtr(uint32_t offset);

    void mapToMemory(uint8_t* target, bool shared);

    void writeData(std::span<const uint8_t> buffer, uint32_t offset = 0);
};

std::string snapshotDataTypeStr(SnapshotDataType dt);

std::string snapshotMergeOpStr(SnapshotMergeOperation op);
}
