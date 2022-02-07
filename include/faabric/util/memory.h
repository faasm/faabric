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
 * Merges the dirty page flags from b into a in place
 */
void mergeDirtyPages(std::vector<char>& a, const std::vector<char>& b);

/*
 * Typedef used to enforce RAII on mmapped memory regions
 */
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
