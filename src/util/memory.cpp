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

// See docs: https://www.kernel.org/doc/html/v5.4/admin-guide/mm/pagemap.html
// and source (grep PM_SOFT_DIRTY):
// https://github.com/torvalds/linux/blob/master/fs/proc/task_mmu.c
#define PAGEMAP_ENTRY_BYTES 8
#define PAGEMAP_SOFT_DIRTY (1Ull << 55)
#define PAGEMAP_EXCLUSIVE_MAP (1Ull << 56)
#define PAGEMAP_FILE (1Ull << 61)

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
        throw std::runtime_error("Failed to write to clear_refs");
    }

    SPDLOG_TRACE("Reset dirty page tracking");

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

std::vector<bool> getPagemapFlags(const uint8_t* ptr,
                                  int nPages,
                                  uint64_t flag,
                                  bool foundFlag)
{
    // Get the pagemap entries
    uintptr_t vptr = (uintptr_t)ptr;
    std::vector<uint64_t> entries = readPagemapEntries(vptr, nPages);

    // Iterate through to get boolean flags
    std::vector<bool> flags(nPages, !foundFlag);
    for (int i = 0; i < nPages; i++) {
        if (entries.at(i) & flag) {
            flags.at(i) = foundFlag;
        }
    }

    return flags;
}

void printFlags(const uint8_t* ptr, int nPages)
{
    // Get the pagemap entries
    uintptr_t vptr = (uintptr_t)ptr;
    std::vector<uint64_t> entries = readPagemapEntries(vptr, nPages);

    // Iterate through to get boolean flags
    for (int i = 0; i < nPages; i++) {
        printf("Page %i: ", i);
        if (entries.at(i) & PAGEMAP_SOFT_DIRTY) {
            printf(" SD ");
        }

        if (entries.at(i) & PAGEMAP_EXCLUSIVE_MAP) {
            printf(" EM ");
        }

        if (entries.at(i) & PAGEMAP_FILE) {
            printf(" F ");
        }

        printf("\n");
    }
}

std::vector<bool> getDirtyPagesForMappedMemory(const uint8_t* ptr, int nPages)
{
    return getPagemapFlags(ptr, nPages, PAGEMAP_FILE, false);
}

std::vector<bool> getDirtyPages(const uint8_t* ptr, int nPages)
{
    return getPagemapFlags(ptr, nPages, PAGEMAP_SOFT_DIRTY, true);
}
}
