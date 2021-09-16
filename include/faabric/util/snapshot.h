#pragma once

#include <memory>
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
    SnapshotDataType dataType;
    SnapshotMergeOperation operation;
};

struct SnapshotDiff
{
    uint32_t offset = 0;
    size_t size = 0;
    const uint8_t* data = nullptr;
    SnapshotDataType dataType;
    SnapshotMergeOperation operation;

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

    void applyDiff(size_t diffOffset, const uint8_t* diffData, size_t diffLen);

  private:
    std::vector<SnapshotMergeRegion> mergeRegions;
};
}
