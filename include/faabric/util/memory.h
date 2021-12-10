#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

namespace faabric::util {

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

bool isPageAligned(void* ptr);

size_t getRequiredHostPages(size_t nBytes);

size_t getRequiredHostPagesRoundDown(size_t nBytes);

size_t alignOffsetDown(size_t offset);

AlignedChunk getPageAlignedChunk(long offset, long length);

// -------------------------
// Dirty pages
// -------------------------
void resetDirtyTracking();

std::vector<int> getDirtyPageNumbers(const uint8_t* ptr, int nPages);

std::vector<std::pair<uint32_t, uint32_t>> getDirtyRegions(const uint8_t* ptr,
                                                           int nPages);
// -------------------------
// Allocation
// -------------------------

typedef std::unique_ptr<uint8_t[], std::function<void(uint8_t*)>>
  OwnedMmapRegion;

OwnedMmapRegion allocateSharedMemory(size_t size);

OwnedMmapRegion allocatePrivateMemory(size_t size);

OwnedMmapRegion allocateVirtualMemory(size_t size);

void claimVirtualMemory(uint8_t* start, size_t size);

void mapMemory(uint8_t* target, size_t size, int fd);

int writeMemoryToFd(const uint8_t* source,
                    size_t size,
                    const std::string& fdLabel);

void appendDataToFd(int fd, size_t oldSize, size_t newSize, uint8_t* newData);
}
