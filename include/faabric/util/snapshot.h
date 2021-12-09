#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <faabric/util/logging.h>
#include <faabric/util/macros.h>

#define FOUR_GB (size_t)(1024L * 1024L * 1024L * 4L)

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
    uint8_t* data = nullptr;
    size_t size = 0;

    SnapshotData() = default;

    SnapshotData(const SnapshotData&);

    SnapshotData& operator=(const SnapshotData&) = delete;

    ~SnapshotData();

    bool isRestorable();

    std::vector<SnapshotDiff> getDirtyRegions();

    std::vector<SnapshotDiff> getChangeDiffs(const uint8_t* updated,
                                             size_t updatedSize);

    void addMergeRegion(uint32_t offset,
                        size_t length,
                        SnapshotDataType dataType,
                        SnapshotMergeOperation operation,
                        bool overwrite = false);

    void clearMergeRegions();

    void setSnapshotSize(size_t newSize);

    void updateFd();

    void writeToFd(const std::string& fdLabel);

    void mapToMemory(uint8_t* target);

    std::map<uint32_t, SnapshotMergeRegion> getMergeRegions();

  private:
    int fd = 0;
    size_t fdSize = 0;

    // Note - we care about the order of this map, as we iterate through it
    // in order of offsets
    std::map<uint32_t, SnapshotMergeRegion> mergeRegions;
};

std::string snapshotDataTypeStr(SnapshotDataType dt);

std::string snapshotMergeOpStr(SnapshotMergeOperation op);
}
