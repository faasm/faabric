#pragma once

#include <cstdint>
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

std::vector<bool> getDirtyPages(const uint8_t* ptr, int nPages);

std::vector<bool> getDirtyPagesForMappedMemory(const uint8_t* ptr, int nPages);

void printFlags(const uint8_t* ptr, int nPages);

}
