#pragma once

#include <string>
#include <vector>

namespace faabric::util {

struct SnapshotDiff
{
    uint32_t offset = 0;
    size_t size = 0;
    const uint8_t* data = nullptr;

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

    std::vector<SnapshotDiff> getDirtyPages();

    std::vector<SnapshotDiff> getChangeDiffs(const uint8_t* updated,
            size_t updatedSize);

    void applyDiff(size_t diffOffset, const uint8_t* diffData, size_t diffLen);
};
}
