#pragma once

#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <functional>
#include <inttypes.h>
#include <memory>
#include <poll.h>
#include <signal.h>
#include <span>
#include <string>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <thread>
#include <unistd.h>
#include <vector>

#include <faabric/util/logging.h>

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

bool isPageAligned(const void* ptr);

size_t getRequiredHostPages(size_t nBytes);

size_t getRequiredHostPagesRoundDown(size_t nBytes);

size_t alignOffsetDown(size_t offset);

AlignedChunk getPageAlignedChunk(long offset, long length);

// -------------------------
// Allocation
// -------------------------
typedef std::unique_ptr<uint8_t[], std::function<void(uint8_t*)>> MemoryRegion;

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
