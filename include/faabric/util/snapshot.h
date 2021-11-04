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

    SnapshotDiff(uint32_t offsetIn, const uint8_t* dataIn, size_t sizeIn)
    {
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

    SnapshotDiff toDiff(const uint8_t* original, const uint8_t* updated)
    {
        const uint8_t* updatedValue = updated + offset;
        const uint8_t* originalValue = original + offset;

        SnapshotDiff diff(offset, updatedValue, length);
        diff.dataType = dataType;
        diff.operation = operation;

        // Modify diff data for certain operations
        switch (dataType) {
            case (SnapshotDataType::Int): {
                int originalInt =
                  *(reinterpret_cast<const int*>(originalValue));
                int updatedInt = *(reinterpret_cast<const int*>(updatedValue));

                if (originalInt == updatedInt) {
                    diff.size = 0;
                    diff.noChange = true;
                    return diff;
                }

                switch (operation) {
                    case (SnapshotMergeOperation::Sum): {
                        // Sums must send the value to be _added_, and
                        // not the final result
                        updatedInt -= originalInt;
                        break;
                    }
                    case (SnapshotMergeOperation::Subtract): {
                        // Subtractions must send the value to be
                        // subtracted, not the result
                        updatedInt = originalInt - updatedInt;
                        break;
                    }
                    case (SnapshotMergeOperation::Product): {
                        // Products must send the value to be
                        // multiplied, not the result
                        updatedInt /= originalInt;
                        break;
                    }
                    case (SnapshotMergeOperation::Max):
                    case (SnapshotMergeOperation::Min):
                        // Min and max don't need to change
                        break;
                    default: {
                        SPDLOG_ERROR("Unhandled integer merge operation: {}",
                                     operation);
                        throw std::runtime_error(
                          "Unhandled integer merge operation");
                    }
                }

                // TODO - somehow avoid casting away the const here?
                // Modify the memory in-place here
                std::memcpy(
                  (uint8_t*)updatedValue, BYTES(&updatedInt), sizeof(int32_t));

                break;
            }
            case (SnapshotDataType::Raw): {
                switch (operation) {
                    case (SnapshotMergeOperation::Ignore): {
                        break;
                    }
                    case (SnapshotMergeOperation::Overwrite): {
                        // TODO - how can we make this fine-grained?
                        // Default behaviour
                        break;
                    }
                    default: {
                        SPDLOG_ERROR("Unhandled raw merge operation: {}",
                                     operation);
                        throw std::runtime_error(
                          "Unhandled raw merge operation");
                    }
                }

                break;
            }
            default: {
                SPDLOG_ERROR("Merge region for unhandled data type: {}",
                             dataType);
                throw std::runtime_error(
                  "Merge region for unhandled data type");
            }
        }
        return diff;
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

    void setIsCustomMerge(bool value);

    bool isCustomMerge();

  private:
    // Note - we care about the order of this map, as we iterate through it
    // in order of offsets
    std::map<uint32_t, SnapshotMergeRegion> mergeRegions;

    bool _isCustomMerge = false;

    std::vector<SnapshotDiff> getCustomDiffs(const uint8_t* updated,
                                             size_t updatedSize);

    std::vector<SnapshotDiff> getStandardDiffs(const uint8_t* updated,
                                               size_t updatedSize);
};

std::string snapshotDataTypeStr(SnapshotDataType dt);

std::string snapshotMergeOpStr(SnapshotMergeOperation op);
}
