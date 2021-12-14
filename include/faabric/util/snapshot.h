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

    // Has same size and max size. Object owns data.
    explicit SnapshotData(size_t sizeIn);

    // Has size and max size. Object owns data.
    SnapshotData(size_t sizeIn, size_t maxSizeIn);

    // Has size and max size equal to size of input data. Object does not own
    // data.
    explicit SnapshotData(std::span<uint8_t> dataIn);

    // Has size equal to size of input data and specified max size. Object does
    // not own data.
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

    void mapToMemoryPrivate(uint8_t* target);

    void mapToMemoryShared(uint8_t* target);

    std::map<uint32_t, SnapshotMergeRegion> getMergeRegions();

  private:
    int fd = -1;

    std::shared_mutex snapMx;

    // If we are the owner, the data pointer's deleter will take care of
    // unmapping the underlying memory. If we are not the owner, the deleter for
    // the pointer will be a noop.
    bool owner = false;
    MemoryRegion _data = nullptr;

    // Note - we care about the order of this map, as we iterate through it
    // in order of offsets
    std::map<uint32_t, SnapshotMergeRegion> mergeRegions;

    uint8_t* validateDataOffset(uint32_t offset);

    void mapToMemory(uint8_t* target, bool shared);
};

std::string snapshotDataTypeStr(SnapshotDataType dt);

std::string snapshotMergeOpStr(SnapshotMergeOperation op);
}
