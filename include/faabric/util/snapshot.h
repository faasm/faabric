#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <span>
#include <string>
#include <vector>

#include <faabric/util/logging.h>
#include <faabric/util/macros.h>
#include <faabric/util/memory.h>

namespace faabric::util {

enum SnapshotDataType
{
    Raw,
    Int
};

enum SnapshotMergeOperation
{
    Overwrite,
    Sum,
    Product,
    Subtract,
    Max,
    Min
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

    void addDiffs(std::vector<SnapshotDiff>& diffs,
                  const uint8_t* original,
                  uint32_t originalSize,
                  const uint8_t* updated,
                  uint32_t dirtyRegionStart,
                  uint32_t dirtyRegionEnd);
};

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

    void mapToMemory(uint8_t* target);

    void addMergeRegion(uint32_t offset,
                        size_t length,
                        SnapshotDataType dataType,
                        SnapshotMergeOperation operation,
                        bool overwrite = false);

    void clearMergeRegions();

    std::map<uint32_t, SnapshotMergeRegion> getMergeRegions();

    size_t getQueuedDiffsCount();

    void queueDiffs(std::span<SnapshotDiff> diffs);

    void writeQueuedDiffs();

    size_t getSize() const { return size; }

    size_t getMaxSize() const { return maxSize; }

  private:
    size_t size = 0;
    size_t maxSize = 0;

    int fd = -1;

    std::shared_mutex snapMx;

    MemoryRegion data = nullptr;

    std::vector<SnapshotDiff> queuedDiffs;

    // Note - we care about the order of this map, as we iterate through it
    // in order of offsets
    std::map<uint32_t, SnapshotMergeRegion> mergeRegions;

    uint8_t* validatedOffsetPtr(uint32_t offset);

    void mapToMemory(uint8_t* target, bool shared);

    void writeData(std::span<const uint8_t> buffer, uint32_t offset = 0);
};

class MemoryView
{
  public:
    // Note - this object is just a view of a section of memory, and does not
    // own the underlying data
    MemoryView() = default;

    explicit MemoryView(std::span<const uint8_t> dataIn);

    std::vector<SnapshotDiff> getDirtyRegions();

    std::vector<SnapshotDiff> diffWithSnapshot(
      std::shared_ptr<SnapshotData> snap);

    std::span<const uint8_t> getData() { return data; }

  private:
    std::span<const uint8_t> data;
};

std::string snapshotDataTypeStr(SnapshotDataType dt);

std::string snapshotMergeOpStr(SnapshotMergeOperation op);
}
