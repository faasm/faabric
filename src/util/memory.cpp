#include <faabric/util/logging.h>
#include <faabric/util/memory.h>

#include <fcntl.h>
#include <stdexcept>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/types.h>

#define CLEAR_REFS "/proc/self/clear_refs"
#define PAGEMAP "/proc/self/pagemap"

#define PAGEMAP_ENTRY_BYTES 8
#define PAGEMAP_SOFT_DIRTY (1Ull << 55)

namespace faabric::util {

// -------------------------
// Alignment
// -------------------------

bool isPageAligned(void* ptr)
{
    return (((uintptr_t)(const void*)(ptr)) % (HOST_PAGE_SIZE) == 0);
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
// Dirty page tracking
// -------------------------

void resetDirtyTracking()
{
    SPDLOG_DEBUG("Resetting dirty tracking");

    FILE* fd = fopen(CLEAR_REFS, "w");
    if (fd == nullptr) {
        SPDLOG_ERROR("Could not open clear_refs ({})", strerror(errno));
        throw std::runtime_error("Could not open clear_refs");
    }

    // Write 4 to the file to track from now on
    // https://www.kernel.org/doc/html/v5.4/admin-guide/mm/soft-dirty.html
    char value[] = "4";
    size_t nWritten = fwrite(value, sizeof(char), 1, fd);
    if (nWritten != 1) {
        SPDLOG_ERROR("Failed to write to clear_refs ({})", nWritten);
        fclose(fd);
        throw std::runtime_error("Failed to write to clear_refs");
    }

    fclose(fd);
}

std::vector<uint64_t> readPagemapEntries(uintptr_t ptr, int nEntries)
{
    // Work out offset for this pointer in the pagemap
    off_t offset = (ptr / getpagesize()) * PAGEMAP_ENTRY_BYTES;

    // Open the pagemap
    FILE* fd = fopen(PAGEMAP, "rb");
    if (fd == nullptr) {
        SPDLOG_ERROR("Could not open pagemap ({})", strerror(errno));
        throw std::runtime_error("Could not open pagemap");
    }

    // Skip to location of this page
    int r = fseek(fd, offset, SEEK_SET);
    if (r < 0) {
        SPDLOG_ERROR("Could not seek pagemap ({})", r);
        throw std::runtime_error("Could not seek pagemap");
    }

    // Read the entries
    std::vector<uint64_t> entries(nEntries, 0);
    int nRead = fread(entries.data(), PAGEMAP_ENTRY_BYTES, nEntries, fd);
    if (nRead != nEntries) {
        SPDLOG_ERROR("Could not read pagemap ({} != {})", nRead, nEntries);
        throw std::runtime_error("Could not read pagemap");
    }

    fclose(fd);

    return entries;
}

std::vector<int> getDirtyPageNumbers(const uint8_t* ptr, int nPages)
{
    uintptr_t vptr = (uintptr_t)ptr;

    // Get the pagemap entries
    std::vector<uint64_t> entries = readPagemapEntries(vptr, nPages);

    // Iterate through to get boolean flags
    std::vector<int> pageNumbers;
    for (int i = 0; i < nPages; i++) {
        if (entries.at(i) & PAGEMAP_SOFT_DIRTY) {
            pageNumbers.emplace_back(i);
        }
    }

    return pageNumbers;
}

std::vector<std::pair<uint32_t, uint32_t>> getDirtyRegions(const uint8_t* ptr,
                                                           int nPages)
{
    std::vector<int> dirtyPages = getDirtyPageNumbers(ptr, nPages);

    // Add a new region for each page, unless the one before it was also dirty,
    // in which case we merge them
    std::vector<std::pair<uint32_t, uint32_t>> regions;
    for (int p = 0; p < dirtyPages.size(); p++) {
        int thisPageNum = dirtyPages.at(p);
        uint32_t thisPageStart = thisPageNum * HOST_PAGE_SIZE;
        uint32_t thisPageEnd = thisPageStart + HOST_PAGE_SIZE;

        if (p > 0 && dirtyPages.at(p - 1) == thisPageNum - 1) {
            // Previous page was also dirty, just update last region
            regions.back().second = thisPageEnd;
        } else {
            // Previous page wasn't dirty, add new region
            regions.emplace_back(thisPageStart, thisPageEnd);
        }
    }

    return regions;
}

// -------------------------
// Allocation
// -------------------------

OwnedMmapRegion doAlloc(size_t size, int prot, int flags)
{
    auto deleter = [size](uint8_t* u) { munmap(u, size); };
    OwnedMmapRegion mem(
        (uint8_t*)::mmap(nullptr, size, prot, flags, -1, 0), deleter);
    return mem;
}

OwnedMmapRegion allocateSharedMemory(size_t size)
{
    if (size < HOST_PAGE_SIZE) {
        SPDLOG_WARN("Allocating less than a page of memory");
    }

    return doAlloc(size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS);
}

OwnedMmapRegion allocateVirtualMemory(size_t size)
{
    return doAlloc(size, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS);
}

void claimVirtualMemory(uint8_t* start, size_t size)
{
    int protectRes = ::mprotect(start, size, PROT_READ | PROT_WRITE);
    if (protectRes != 0) {
        SPDLOG_ERROR("Failed claiming virtual memory: {}", strerror(errno));
        throw std::runtime_error("Failed claiming virtual memory");
    }
}

void mapMemory(uint8_t* target, size_t size, int fd)
{
    if (!faabric::util::isPageAligned((void*)target)) {
        SPDLOG_ERROR("Mapping memory to non page-aligned address");
        throw std::runtime_error("Mapping memory to non page-aligned address");
    }

    if (fd == 0) {
        SPDLOG_ERROR("Attempting to map to zero fd");
        throw std::runtime_error("Attempting to map to zero fd");
    }

    // Make mmap call to do the mapping
    void* mmapRes = ::mmap(
      target, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_FIXED, fd, 0);

    if (mmapRes == MAP_FAILED) {
        SPDLOG_ERROR("mapping memory to fd {} failed: {} ({})",
                     fd,
                     errno,
                     ::strerror(errno));
        throw std::runtime_error("mmapping memory failed");
    }
}

int writeMemoryToFd(const uint8_t* source, size_t size, const std::string& fdLabel)
{
    int fd = ::memfd_create(fdLabel.c_str(), 0);
    if (fd == -1) {
        SPDLOG_ERROR("Failed to create file descriptor: {}", strerror(errno));
        throw std::runtime_error("Failed to create file descriptor");
    }

    // Make the fd big enough
    int ferror = ::ftruncate(fd, size);
    if (ferror != 0) {
        SPDLOG_ERROR("ftruncate call failed with error {}", ferror);
        throw std::runtime_error("Failed writing memory to fd (ftruncate)");
    }

    // Write the data
    ssize_t werror = ::write(fd, source, size);
    if (werror == -1) {
        SPDLOG_ERROR("Write call failed with error {}", werror);
        throw std::runtime_error("Failed writing memory to fd (write)");
    }

    return fd;
}

void appendDataToFd(int fd, size_t oldSize, size_t newSize, uint8_t* data)
{
    if (newSize == oldSize) {
        return;
    }

    // Extend the fd
    int ferror = ::ftruncate(fd, newSize);
    if (ferror != 0) {
        SPDLOG_ERROR("Extending with ftruncate failed with error {}", ferror);
        throw std::runtime_error("Failed appending data to fd (ftruncate)");
    }

    // Skip to the end of the old data
    int seekRes = ::lseek(fd, oldSize, SEEK_SET);
    if (seekRes == -1) {
        SPDLOG_ERROR("lseek call failed with error {}", strerror(errno));
        throw std::runtime_error("Failed appending data to fd");
    }

    // Write the data
    uint8_t* newDataStart = data + oldSize;
    size_t newDataSize = newSize - oldSize;
    ssize_t werror = ::write(fd, newDataStart, newDataSize);
    if (werror == -1) {
        SPDLOG_ERROR("Appending with write failed with error {}", werror);
        throw std::runtime_error("Failed appending memory to fd (write)");
    }
}
}
