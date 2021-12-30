#include <faabric/util/dirty.h>

namespace faabric::util {

DirtyPageTracker& getDirtyPageTracker()
{
    static SoftPTEDirtyTracker spte;
    static SegfaultDirtyTracker mprot;

    std::string trackMode = faabric::util::getSystemConfig().dirtyTrackingMode;
    if (trackMode == "softpte") {
        return spte;
    } else if (trackMode == "sigseg") {
        return mprot;
    } else {
        throw std::runtime_error("Unrecognised dirty tracking mode");
    }
}

// ----------------------------------
// Soft dirty PTE
// ----------------------------------

SoftPTEDirtyTracker::SoftPTEDirtyTracker()
{
    f = ::fopen(CLEAR_REFS, "w");
    if (f == nullptr) {
        SPDLOG_ERROR("Could not open clear_refs ({})", strerror(errno));
        throw std::runtime_error("Could not open clear_refs");
    }
}

SoftPTEDirtyTracker::~SoftPTEDirtyTracker()
{
    ::fclose(f);
}

void SoftPTEDirtyTracker::clearAll()
{
    // Write 4 to the file to reset and start tracking
    // https://www.kernel.org/doc/html/v5.4/admin-guide/mm/soft-dirty.html
    char value[] = "4";
    size_t nWritten = ::fwrite(value, sizeof(char), 1, f);

    if (nWritten != 1) {
        SPDLOG_ERROR("Failed to write to clear_refs ({})", nWritten);
        ::fclose(f);
        throw std::runtime_error("Failed to write to clear_refs");
    }

    ::rewind(f);
}

void SoftPTEDirtyTracker::restartTracking(std::span<uint8_t> region)
{
    clearAll();
}

void SoftPTEDirtyTracker::startTracking(std::span<uint8_t> region)
{
    clearAll();
}

void SoftPTEDirtyTracker::stopTracking(std::span<uint8_t> region)
{
    clearAll();
}

std::vector<std::pair<uint32_t, uint32_t>> SoftPTEDirtyTracker::getDirtyOffsets(
  std::span<uint8_t> region)
{
    // Get dirty regions
    int nPages = getRequiredHostPages(region.size());
    std::vector<int> dirtyPageNumbers =
      getDirtyPageNumbers(region.data(), nPages);

    SPDLOG_DEBUG(
      "Region has {}/{} dirty pages", dirtyPageNumbers.size(), nPages);

    std::vector<std::pair<uint32_t, uint32_t>> regions =
      getDirtyRegions(region.data(), nPages);

    return regions;
}

std::vector<uint64_t> SoftPTEDirtyTracker::readPagemapEntries(uintptr_t ptr,
                                                              int nEntries)
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

std::vector<int> SoftPTEDirtyTracker::getDirtyPageNumbers(const uint8_t* ptr,
                                                          int nPages)
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

std::vector<std::pair<uint32_t, uint32_t>> SoftPTEDirtyTracker::getDirtyRegions(
  const uint8_t* ptr,
  int nPages)
{
    std::vector<int> dirtyPages = getDirtyPageNumbers(ptr, nPages);

    // Add a new region for each page, unless the one before it was also
    // dirty, in which case we merge them
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

// ------------------------------
// Mprotect
// ------------------------------

SegfaultDirtyTracker::SegfaultDirtyTracker()
{
    // Set up sig handler
    struct sigaction sa;

    sa.sa_flags = SA_SIGINFO;
    sigemptyset(&sa.sa_mask);
    sa.sa_sigaction = handler;
    if (sigaction(SIGSEGV, &sa, NULL) == -1) {
        throw std::runtime_error("Failed sigaction");
    }
}

void SegfaultDirtyTracker::handler(int sig, siginfo_t* si, void* unused)
{
    // TODO - work out the page that's dirtied

    // TODO - register dirty page somewhere

    // TODO - reset mprotect to READ/WRITE
}

void SegfaultDirtyTracker::clearAll()
{
    // TODO
}

void SegfaultDirtyTracker::restartTracking(std::span<uint8_t> region)
{
    // TODO
}

void SegfaultDirtyTracker::startTracking(std::span<uint8_t> region)
{
    if (::mprotect(region.data(), region.size(), PROT_READ) == -1) {
        throw std::runtime_error("Failed mprotect");
    }
}

void SegfaultDirtyTracker::stopTracking(std::span<uint8_t> region)
{
    if (::mprotect(region.data(), region.size(), PROT_READ | PROT_WRITE) ==
        -1) {
        throw std::runtime_error("Failed mprotect");
    }
}

std::vector<std::pair<uint32_t, uint32_t>>
SegfaultDirtyTracker::getDirtyOffsets(std::span<uint8_t> region)
{
    std::vector<std::pair<uint32_t, uint32_t>> dirty;
    return dirty;
}
}
