#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
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

    explicit SnapshotData(size_t sizeIn);

    explicit SnapshotData(std::vector<uint8_t> dataIn);

    SnapshotData(size_t sizeIn, size_t maxSizeIn);

    SnapshotData(uint8_t* dataIn, size_t sizeIn);

    SnapshotData(uint8_t* dataIn, size_t sizeIn, size_t maxSizeIn);

    SnapshotData(const SnapshotData&) = delete;

    SnapshotData& operator=(const SnapshotData&) = delete;

    ~SnapshotData();

    bool isRestorable();

    void makeRestorable(const std::string& fdLabel);

    void setSnapshotSize(size_t newSize);

    void copyInData(std::vector<uint8_t> buffer, uint32_t offset = 0);

    void copyInData(uint8_t* buffer, size_t bufferSize, uint32_t offset = 0);

    const uint8_t* getDataPtr(uint32_t offset = 0);

    uint8_t* getMutableDataPtr(uint32_t offset = 0);

    std::vector<uint8_t> getDataCopy();

    std::vector<uint8_t> getDataCopy(uint32_t offset, size_t dataSize);

    std::vector<SnapshotDiff> getDirtyRegions();

    std::vector<SnapshotDiff> getChangeDiffs(const uint8_t* updated,
                                             size_t updatedSize);

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
    size_t fdSize = 0;

    std::shared_mutex snapMx;

    bool owner = false;
    uint8_t* rawData = nullptr;
    OwnedMmapRegion ownedData = nullptr;

    // Note - we care about the order of this map, as we iterate through it
    // in order of offsets
    std::map<uint32_t, SnapshotMergeRegion> mergeRegions;
};

std::string snapshotDataTypeStr(SnapshotDataType dt);

std::string snapshotMergeOpStr(SnapshotMergeOperation op);
}
