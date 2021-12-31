#include <faabric/util/logging.h>
#include <faabric/util/memory.h>
#include <faabric/util/timing.h>

#include <fcntl.h>
#include <stdexcept>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/types.h>

namespace faabric::util {

std::vector<OffsetMemoryRegion> dedupeMemoryRegions(
  std::vector<OffsetMemoryRegion>& regions)
{
    if (regions.empty()) {
        return {};
    }

    std::vector<OffsetMemoryRegion> deduped;

    // Sort in place
    std::sort(std::begin(regions),
              std::end(regions),
              [](OffsetMemoryRegion& a, OffsetMemoryRegion& b) {
                  return a.offset < b.offset;
              });

    deduped.push_back(regions.front());
    uint32_t lastOffset = regions.front().offset;
    for (int i = 1; i < regions.size(); i++) {
        const auto& r = regions.at(i);
        assert(r.offset >= lastOffset);

        if (r.offset > lastOffset) {
            deduped.push_back(r);
            lastOffset = r.offset;
        } else if (deduped.back().data.size() < r.data.size()) {
            deduped.pop_back();
            deduped.push_back(r);
        }
    }

    return deduped;
}
// -------------------------
// Alignment
// -------------------------

bool isPageAligned(const void* ptr)
{
    return (((uintptr_t)(ptr)) % (HOST_PAGE_SIZE) == 0);
}

size_t getRequiredHostPages(size_t nBytes)
{
    // Rounding up
    size_t nHostPages = (nBytes + faabric::util::HOST_PAGE_SIZE - 1) /
                        faabric::util::HOST_PAGE_SIZE;
    return nHostPages;
}

size_t getRequiredHostPagesRoundDown(size_t nBytes)
{
    // Relying on integer division rounding down
    size_t nHostPages = nBytes / faabric::util::HOST_PAGE_SIZE;
    return nHostPages;
}

size_t alignOffsetDown(size_t offset)
{
    size_t nHostPages = getRequiredHostPagesRoundDown(offset);
    return nHostPages * faabric::util::HOST_PAGE_SIZE;
}

AlignedChunk getPageAlignedChunk(long offset, long length)
{
    // Calculate the page boundaries
    auto nPagesOffset =
      (long)faabric::util::getRequiredHostPagesRoundDown(offset);
    auto nPagesUpper =
      (long)faabric::util::getRequiredHostPages(offset + length);
    long nPagesLength = nPagesUpper - nPagesOffset;

    long nBytesLength = nPagesLength * faabric::util::HOST_PAGE_SIZE;

    long nBytesOffset = nPagesOffset * faabric::util::HOST_PAGE_SIZE;

    // Note - this value is the offset from the base of the new region
    long shiftedOffset = offset - nBytesOffset;

    AlignedChunk c{
        .originalOffset = offset,
        .originalLength = length,
        .nBytesOffset = nBytesOffset,
        .nBytesLength = nBytesLength,
        .nPagesOffset = nPagesOffset,
        .nPagesLength = nPagesLength,
        .offsetRemainder = shiftedOffset,
    };

    return c;
}

// -------------------------
// Allocation
// -------------------------

MemoryRegion doAlloc(size_t size, int prot, int flags)
{
    auto deleter = [size](uint8_t* u) { munmap(u, size); };
    MemoryRegion mem((uint8_t*)::mmap(nullptr, size, prot, flags, -1, 0),
                     deleter);

    if (mem.get() == MAP_FAILED) {
        SPDLOG_ERROR("Allocating memory with mmap failed: {} ({})",
                     errno,
                     ::strerror(errno));
        throw std::runtime_error("Allocating memory failed");
    }

    return mem;
}

MemoryRegion allocatePrivateMemory(size_t size)
{
    return doAlloc(size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS);
}

MemoryRegion allocateSharedMemory(size_t size)
{
    return doAlloc(size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS);
}

MemoryRegion allocateVirtualMemory(size_t size)
{
    return doAlloc(size, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS);
}

void claimVirtualMemory(std::span<uint8_t> region)
{
    int protectRes =
      ::mprotect(region.data(), region.size(), PROT_READ | PROT_WRITE);
    if (protectRes != 0) {
        SPDLOG_ERROR("Failed claiming virtual memory: {}", strerror(errno));
        throw std::runtime_error("Failed claiming virtual memory");
    }
}

void mapMemory(std::span<uint8_t> target, int fd, int flags)
{
    if (!faabric::util::isPageAligned((void*)target.data())) {
        SPDLOG_ERROR("Mapping memory to non page-aligned address");
        throw std::runtime_error("Mapping memory to non page-aligned address");
    }

    if (fd <= 0) {
        SPDLOG_ERROR("Mapping invalid or zero fd ({})", fd);
        throw std::runtime_error("Invalid fd for mapping");
    }

    void* mmapRes = ::mmap(
      target.data(), target.size(), PROT_READ | PROT_WRITE, flags, fd, 0);

    if (mmapRes == MAP_FAILED) {
        SPDLOG_ERROR("mapping memory to fd {} failed: {} ({})",
                     fd,
                     errno,
                     ::strerror(errno));
        throw std::runtime_error("mmapping memory failed");
    }
}

void mapMemoryPrivate(std::span<uint8_t> target, int fd)
{
    mapMemory(target, fd, MAP_PRIVATE | MAP_FIXED);
}

void mapMemoryShared(std::span<uint8_t> target, int fd)
{
    mapMemory(target, fd, MAP_SHARED | MAP_FIXED);
}

void resizeFd(int fd, size_t size)
{
    int ferror = ::ftruncate(fd, size);
    if (ferror != 0) {
        SPDLOG_ERROR("ftruncate call failed with error {}", ferror);
        throw std::runtime_error("Failed writing memory to fd (ftruncate)");
    }
}

void writeToFd(int fd, off_t offset, std::span<const uint8_t> data)
{
    // Seek to the right point
    off_t lseekRes = ::lseek(fd, offset, SEEK_SET);
    if (lseekRes == -1) {
        SPDLOG_ERROR("Failed to set fd {} to offset {}", fd, offset);
        throw std::runtime_error("Failed changing fd size");
    }

    // Write the data
    ssize_t werror = ::write(fd, data.data(), data.size());
    if (werror == -1) {
        SPDLOG_ERROR("Write call failed with error {}", werror);
        throw std::runtime_error("Failed writing memory to fd (write)");
    }

    // Set back to end
    ::lseek(fd, 0, SEEK_END);
}

int createFd(size_t size, const std::string& fdLabel)
{
    // Create new fd
    int fd = ::memfd_create(fdLabel.c_str(), 0);
    if (fd == -1) {
        SPDLOG_ERROR("Failed to create file descriptor: {}", strerror(errno));
        throw std::runtime_error("Failed to create file descriptor");
    }

    // Make the fd big enough
    resizeFd(fd, size);

    return fd;
}

void appendDataToFd(int fd, std::span<uint8_t> data)
{
    off_t oldSize = ::lseek(fd, 0, SEEK_END);
    if (oldSize == -1) {
        SPDLOG_ERROR("lseek to get old size failed: {}", strerror(errno));
        throw std::runtime_error("Failed seeking existing size of fd");
    }

    if (data.empty()) {
        return;
    }

    // Extend the fd
    off_t newSize = oldSize + data.size();
    int ferror = ::ftruncate(fd, newSize);
    if (ferror != 0) {
        SPDLOG_ERROR("Extending with ftruncate failed with error {}", ferror);
        throw std::runtime_error("Failed appending data to fd (ftruncate)");
    }

    // Skip to the end of the old data
    off_t seekRes = ::lseek(fd, oldSize, SEEK_SET);
    if (seekRes == -1) {
        SPDLOG_ERROR("lseek call failed with error {}", strerror(errno));
        throw std::runtime_error("Failed appending data to fd");
    }

    // Write the data
    ssize_t werror = ::write(fd, data.data(), data.size());
    if (werror == -1) {
        SPDLOG_ERROR("Appending with write failed with error {}", werror);
        throw std::runtime_error("Failed appending memory to fd (write)");
    }
}
}
