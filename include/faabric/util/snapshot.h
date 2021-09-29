#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

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

struct SnapshotMergeRegion
{
    uint32_t offset = 0;
    size_t length = 0;
    SnapshotDataType dataType = SnapshotDataType::Raw;
    SnapshotMergeOperation operation = SnapshotMergeOperation::Overwrite;
};

class SnapshotDiff
{
  public:
    uint32_t offset = 0;
    size_t size = 0;
    const uint8_t* data = nullptr;
    SnapshotDataType dataType = SnapshotDataType::Raw;
    SnapshotMergeOperation operation = SnapshotMergeOperation::Overwrite;

    SnapshotDiff() = default;

    SnapshotDiff(uint32_t offsetIn, const uint8_t* dataIn, size_t sizeIn)
    {
        offset = offsetIn;
        data = dataIn;
        size = sizeIn;
    }
};

class SnapshotData
{
  public:
    size_t size = 0;
    uint8_t* data = nullptr;
    int fd = 0;

    SnapshotData() = default;

    std::vector<SnapshotDiff> getDirtyPages();

    std::vector<SnapshotDiff> getChangeDiffs(const uint8_t* updated,
                                             size_t updatedSize);

    void addMergeRegion(uint32_t offset,
                        size_t length,
                        SnapshotDataType dataType,
                        SnapshotMergeOperation operation);

  private:
    // Note - we care about the order of this map, as we iterate through it in
    // order of offsets
    std::map<uint32_t, SnapshotMergeRegion> mergeRegions;
};
}
