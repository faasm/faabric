#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <faabric/util/logging.h>
#include <faabric/util/macros.h>

namespace faabric::util {

enum SnapshotDataType
{
    Raw,
    Int
};

enum SnapshotMergeOperation
{
    Overwrite,
    Ignore,
    Sum,
    Product,
    Subtract,
    Max,
    Min
};

class SnapshotDiff
{
  public:
    SnapshotDataType dataType = SnapshotDataType::Raw;
    SnapshotMergeOperation operation = SnapshotMergeOperation::Overwrite;
    uint32_t offset = 0;
    size_t size = 0;
    const uint8_t* data = nullptr;

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
                  const uint8_t* updated);
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
                        SnapshotMergeOperation operation,
                        bool overwrite = false);

  private:
    // Note - we care about the order of this map, as we iterate through it
    // in order of offsets
    std::map<uint32_t, SnapshotMergeRegion> mergeRegions;
};

std::string snapshotDataTypeStr(SnapshotDataType dt);

std::string snapshotMergeOpStr(SnapshotMergeOperation op);
}
