#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <unistd.h>
#include <vector>

namespace faabric::util {

/*
 * Represents a view of a segment of memory that's offset from the base of a
 * larger linear memory region.
 */
class OffsetMemoryRegion
{
  public:
    OffsetMemoryRegion(uint32_t offsetIn, std::span<uint8_t> dataIn)
      : offset(offsetIn)
      , data(dataIn)
    {}

    bool operator==(const OffsetMemoryRegion& other) const
    {
        return other.offset == offset && other.data.size() == data.size() &&
               other.data.data() == data.data();
    }

    uint32_t offset = 0;
    std::span<uint8_t> data;
};

std::vector<OffsetMemoryRegion> dedupeMemoryRegions(
  std::vector<OffsetMemoryRegion>& regions);

typedef std::unique_ptr<uint8_t[], std::function<void(uint8_t*)>> MemoryRegion;

// -------------------------
// Alignment
// -------------------------
struct AlignedChunk
{
    long originalOffset = 0;
    long originalLength = 0;
    long nBytesOffset = 0;
    long nBytesLength = 0;
    long nPagesOffset = 0;
    long nPagesLength = 0;
    long offsetRemainder = 0;
};

static const long HOST_PAGE_SIZE = sysconf(_SC_PAGESIZE);

bool isPageAligned(const void* ptr);

size_t getRequiredHostPages(size_t nBytes);

size_t getRequiredHostPagesRoundDown(size_t nBytes);

size_t alignOffsetDown(size_t offset);

AlignedChunk getPageAlignedChunk(long offset, long length);

// -------------------------
// Allocation
// -------------------------

MemoryRegion allocatePrivateMemory(size_t size);

MemoryRegion allocateSharedMemory(size_t size);

MemoryRegion allocateVirtualMemory(size_t size);

void claimVirtualMemory(std::span<uint8_t> region);

void mapMemoryPrivate(std::span<uint8_t> target, int fd);

void mapMemoryShared(std::span<uint8_t> target, int fd);

void resizeFd(int fd, size_t size);

void writeToFd(int fd, off_t offset, std::span<const uint8_t> data);

int createFd(size_t size, const std::string& fdLabel);

void appendDataToFd(int fd, std::span<uint8_t> data);
}
