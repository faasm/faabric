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
    const uint8_t* data = nullptr;
    size_t size = 0;
    SnapshotDataType dataType = SnapshotDataType::Raw;
    SnapshotMergeOperation operation = SnapshotMergeOperation::Overwrite;
    uint32_t offset = 0;

    bool noChange = false;

    SnapshotDiff() = default;

    SnapshotDiff(SnapshotDataType dataTypeIn,
                 SnapshotMergeOperation operationIn,
                 uint32_t offsetIn,
                 const uint8_t* dataIn,
                 size_t sizeIn)
    {
        dataType = dataTypeIn;
        operation = operationIn;
        offset = offsetIn;
        data = dataIn;
        size = sizeIn;
    }
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
    size_t size = 0;
    size_t maxSize = 0;

    SnapshotData() = default;

    // Note - if providing data to one of these constructors, the snapshot will
    // not take ownership of the lifecycle of that data.
    // Only when just providing dimensions will the object create and own the
    // underlying buffer.

    // Same size and max size. Object owns data.
    explicit SnapshotData(size_t sizeIn);

    // Given size and max size. Object owns data.
    SnapshotData(size_t sizeIn, size_t maxSizeIn);

    // Object does not own data, has size and max size equal to size of input
    // data.
    explicit SnapshotData(std::span<uint8_t> dataIn);

    // Object does not own data, has size equal to size of input data and given
    // max size.
    SnapshotData(std::span<uint8_t> dataIn, size_t maxSizeIn);

    SnapshotData(const SnapshotData&) = delete;

    SnapshotData& operator=(const SnapshotData&) = delete;

    ~SnapshotData();

    bool isRestorable();

    bool isOwner();

    void makeRestorable(const std::string& fdLabel);

    void copyInData(std::span<uint8_t> buffer, uint32_t offset = 0);

    const uint8_t* getDataPtr(uint32_t offset = 0);

    uint8_t* getMutableDataPtr(uint32_t offset = 0);

    std::vector<uint8_t> getDataCopy();

    std::vector<uint8_t> getDataCopy(uint32_t offset, size_t dataSize);

    std::vector<SnapshotDiff> getDirtyRegions();

    std::vector<SnapshotDiff> getChangeDiffs(std::span<const uint8_t> updated);

    void addMergeRegion(uint32_t offset,
                        size_t length,
                        SnapshotDataType dataType,
                        SnapshotMergeOperation operation,
                        bool overwrite = false);

    void clearMergeRegions();

    void mapToMemory(uint8_t* target);

    std::map<uint32_t, SnapshotMergeRegion> getMergeRegions();

  private:
    int fd = 0;

    std::shared_mutex snapMx;

    // If we are the owner, the data pointer's deleter will take care of
    // unmapping the underlying memory. If we are not the owner, the deleter for
    // the pointer will be a noop.
    bool owner = false;
    MemoryRegion _data = nullptr;

    uint8_t* validateDataOffset(uint32_t offset);

    // Note - we care about the order of this map, as we iterate through it
    // in order of offsets
    std::map<uint32_t, SnapshotMergeRegion> mergeRegions;
};

std::string snapshotDataTypeStr(SnapshotDataType dt);

std::string snapshotMergeOpStr(SnapshotMergeOperation op);
}
