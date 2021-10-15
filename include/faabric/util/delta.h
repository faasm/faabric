#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <utility>
#include <vector>

namespace faabric::util {

struct DeltaSettings
{
    // pages=SIZE;
    bool usePages = true;
    size_t pageSize = 4096;
    // xor;
    bool xorWithOld = true;
    // zstd=LEVEL;
    bool useZstd = true;
    int zstdLevel = 1;

    explicit DeltaSettings(const std::string& definition);
    std::string toString() const;
};

inline constexpr uint8_t DELTA_PROTOCOL_VERSION = 1;
inline constexpr int DELTA_ZSTD_COMPRESS_LEVEL = 1;

enum DeltaCommand : uint8_t
{
    // followed by u32(total size)
    DELTACMD_TOTAL_SIZE = 0x00,
    // followed by u64(compressed length), u64(decompressed length),
    // bytes(compressed commands)
    DELTACMD_ZSTD_COMPRESSED_COMMANDS = 0x01,
    // followed by u32(offset), u32(length), bytes(data)
    DELTACMD_DELTA_OVERWRITE = 0x02,
    // followed by u32(offset), u32(length), bytes(data)
    DELTACMD_DELTA_XOR = 0x03,
    // final command
    DELTACMD_END = 0xFE,
};

std::vector<uint8_t> serializeDelta(
  const DeltaSettings& cfg,
  const uint8_t* oldDataStart,
  size_t oldDataLen,
  const uint8_t* newDataStart,
  size_t newDataLen,
  const std::vector<std::pair<uint32_t, uint32_t>>* excludedPtrLens = nullptr);

void applyDelta(const std::vector<uint8_t>& delta,
                std::function<void(uint32_t)> setDataSize,
                std::function<uint8_t*()> getDataPointer);

}
