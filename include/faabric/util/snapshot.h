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

// This paramter controls the step size in the array comparison function. The
// function will normally be comparing 4kB pages, and when it detects a change
// within a chunk, it will byte-wise compare that chunk
#define ARRAY_COMP_CHUNK_SIZE 128

/**
 * Defines the permitted datatypes for snapshot diffs. Each has a predefined
 * length, except for the raw option which is used for generic streams of bytes.
 */
enum SnapshotDataType
{
    Raw,
    Bool,
    Int,
    Long,
    Float,
    Double
};

/**
 * Defines the operation to perform when merging the diff with the original
 * snapshot.
 *
 * WARNING: this enum forms part of the API with executing applications, so make
 * sure they are updated accordingly when it's changed.
 */
enum SnapshotMergeOperation
{
    Bytewise,
    Sum,
    Product,
    Subtract,
    Max,
    Min,
    Ignore,
    XOR
};

/**
 * Represents a modification to a snapshot (a.k.a. a dirty region). Specifies
 * the modified data, as well as its offset within the snapshot.
 *
 * Each diff does not own the data, it just provides a span pointing at the original
 * data.
 */
class SnapshotDiff
{
  public:
    SnapshotDiff() = default;

    SnapshotDiff(
      SnapshotDataType dataTypeIn,
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
    SnapshotMergeOperation operation = SnapshotMergeOperation::Bytewise;
    uint32_t offset = 0;

    std::span<const uint8_t> data;
};

/*
 * Appends a list of snapshot diffs for any bytes differing between the two
 * arrays.
 *
 * The function compares chunks of bytes at a time, if there are no differences,
 * it skips to the next chunk. If one or more differences are detected in a
 * chunk, it does a byte-wise comparison on that chunk.
 *
 * The performance of this will vary depending on how sparse the diffs are, and
 * on the chunk size itself. A larger chunk size is optimal for very sparse
 * changes, but it makes the byte-wise comparision more intensive (as we're
 * byte-wise comparing more bytes).
 */
void diffArrayRegions(std::vector<SnapshotDiff>& diffs,
                      uint32_t startOffset,
                      uint32_t endOffset,
                      std::span<const uint8_t> a,
                      std::span<const uint8_t> b);

/**
 * Defines how diffs in the given snapshot region should be interpreted wrt the
 * original snapshot.
 *
 * For example, it may specify that the change should be treated as an integer
 * that needs to be summed (i.e. the diff is the current value minus the
 * original), or just as a region of raw bytes that needs to be XORed with the
 * original.
 *
 * A merge region specifies an offset, length, data type and operation, e.g. an
 * integer to be summed at offset 100 with a length of 4.
 */
class SnapshotMergeRegion
{
  public:
    uint32_t offset = 0;
    size_t length = 0;
    SnapshotDataType dataType = SnapshotDataType::Raw;
    SnapshotMergeOperation operation = SnapshotMergeOperation::Bytewise;

    SnapshotMergeRegion() = default;

    SnapshotMergeRegion(uint32_t offsetIn,
                        size_t lengthIn,
                        SnapshotDataType dataTypeIn,
                        SnapshotMergeOperation operationIn);

    void addDiffs(std::vector<SnapshotDiff>& diffs,
                  std::span<const uint8_t> originalData,
                  std::span<uint8_t> updatedData,
                  const std::vector<char>& dirtyRegions);

    /**
     * This allows us to sort the merge regions which is important for diffing
     * purposes.
     */
    bool operator<(const SnapshotMergeRegion& other) const
    {
        return (offset < other.offset);
    }

    bool operator==(const SnapshotMergeRegion& other) const
    {
        return offset == other.offset && length == other.length &&
               dataType == other.dataType && operation == other.operation;
    }
};

/*
 * Calculates a diff value that can later be merged into the master copy of the
 * given snapshot. It will be used on remote hosts to calculate the diffs that
 * are to be sent back to the master host.
 */
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

/*
 * Applies a diff value to the master copy of a snapshot, where the diff has
 * been calculated based on a change made to another copy of the same snapshot.
 */
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
                        SnapshotMergeOperation operation);

    void fillGapsWithBytewiseRegions();

    void clearMergeRegions();

    std::vector<SnapshotMergeRegion> getMergeRegions();

    size_t getQueuedDiffsCount();

    void applyDiffs(const std::vector<SnapshotDiff>& diffs);

    void applyDiff(const SnapshotDiff& diff);

    void queueDiffs(const std::vector<SnapshotDiff>& diffs);

    int writeQueuedDiffs();

    size_t getSize() const { return size; }

    size_t getMaxSize() const { return maxSize; }

    // Returns a list of changes that have been made to the snapshot since the
    // last time the list was cleared.
    std::vector<SnapshotDiff> getTrackedChanges();

    // Clears the list of tracked changes.
    void clearTrackedChanges();

    // Returns the list of changes in the given dirty regions versus their
    // original value in the snapshot, based on the merge regions set on this
    // snapshot.
    std::vector<faabric::util::SnapshotDiff> diffWithDirtyRegions(
      std::span<uint8_t> updated,
      const std::vector<char>& dirtyRegions);

  private:
    size_t size = 0;
    size_t maxSize = 0;

    int fd = -1;

    std::shared_mutex snapMx;

    MemoryRegion data = nullptr;

    std::vector<SnapshotDiff> queuedDiffs;

    std::vector<std::pair<uint32_t, uint32_t>> trackedChanges;

    std::vector<SnapshotMergeRegion> mergeRegions;

    uint8_t* validatedOffsetPtr(uint32_t offset);

    void mapToMemory(uint8_t* target, bool shared);

    void writeData(std::span<const uint8_t> buffer, uint32_t offset = 0);

    void xorData(std::span<const uint8_t> buffer, uint32_t offset = 0);

    void checkWriteExtension(std::span<const uint8_t> buffer, uint32_t offset);
};

std::string snapshotDataTypeStr(SnapshotDataType dt);

std::string snapshotMergeOpStr(SnapshotMergeOperation op);
}
