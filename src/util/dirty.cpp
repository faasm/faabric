#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <inttypes.h>
#include <memory>
#include <signal.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <thread>
#include <unistd.h>
#include <vector>

#include <faabric/util/crash.h>
#include <faabric/util/dirty.h>
#include <faabric/util/memory.h>
#include <faabric/util/testing.h>

namespace faabric::util {

DirtyTracker& getDirtyTracker()
{
    static SoftPTEDirtyTracker softpte;
    static SegfaultDirtyTracker sigseg;

    std::string trackMode = faabric::util::getSystemConfig().dirtyTrackingMode;
    if (trackMode == "softpte") {
        return softpte;
    } else if (trackMode == "segfault") {
        sigseg.reinitialise();
        return sigseg;
    } else {
        throw std::runtime_error("Unrecognised dirty tracking mode");
    }
}

// ----------------------------------
// Soft dirty PTE
// ----------------------------------

SoftPTEDirtyTracker::SoftPTEDirtyTracker()
{
    clearRefsFile = ::fopen(CLEAR_REFS, "w");
    if (clearRefsFile == nullptr) {
        SPDLOG_ERROR("Could not open clear_refs ({})", strerror(errno));
        throw std::runtime_error("Could not open clear_refs");
    }
}

SoftPTEDirtyTracker::~SoftPTEDirtyTracker()
{
    ::fclose(clearRefsFile);
}

void SoftPTEDirtyTracker::clearAll()
{
    // Write 4 to the file to reset and start tracking
    // https://www.kernel.org/doc/html/v5.4/admin-guide/mm/soft-dirty.html
    char value[] = "4";
    size_t nWritten = ::fwrite(value, sizeof(char), 1, clearRefsFile);

    if (nWritten != 1) {
        SPDLOG_ERROR("Failed to write to clear_refs ({})", nWritten);
        ::fclose(clearRefsFile);
        throw std::runtime_error("Failed to write to clear_refs");
    }

    ::rewind(clearRefsFile);
}

void SoftPTEDirtyTracker::startTracking(std::span<uint8_t> region)
{
    clearAll();
}

void SoftPTEDirtyTracker::startThreadLocalTracking(std::span<uint8_t> region)
{
    // Do nothing
}

void SoftPTEDirtyTracker::stopTracking(std::span<uint8_t> region)
{
    // Do nothing
}

void SoftPTEDirtyTracker::stopThreadLocalTracking(std::span<uint8_t> region)
{
    // Do nothing
}

std::vector<OffsetMemoryRegion> SoftPTEDirtyTracker::getDirtyOffsets(
  std::span<uint8_t> region)
{
    // Get dirty regions
    int nPages = getRequiredHostPages(region.size());
    std::vector<int> dirtyPageNumbers =
      getDirtyPageNumbers(region.data(), nPages);

    SPDLOG_DEBUG(
      "Region has {}/{} dirty pages", dirtyPageNumbers.size(), nPages);

    std::vector<OffsetMemoryRegion> regions =
      getDirtyRegions(region.data(), nPages);

    return regions;
}

std::vector<OffsetMemoryRegion> SoftPTEDirtyTracker::getBothDirtyOffsets(
  std::span<uint8_t> region)
{
    return getDirtyOffsets(region);
}

std::vector<OffsetMemoryRegion> SoftPTEDirtyTracker::getThreadLocalDirtyOffsets(
  std::span<uint8_t> region)
{
    return {};
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

std::vector<OffsetMemoryRegion> SoftPTEDirtyTracker::getDirtyRegions(
  uint8_t* ptr,
  int nPages)
{
    std::span<uint8_t> memView(ptr, nPages * HOST_PAGE_SIZE);
    std::vector<int> dirtyPages = getDirtyPageNumbers(ptr, nPages);

    // Add a new region for each page, unless the one before it was also
    // dirty, in which case we extend the previous one
    std::vector<OffsetMemoryRegion> regions;
    for (int p = 0; p < dirtyPages.size(); p++) {
        int thisPageNum = dirtyPages.at(p);
        uint32_t thisPageStart = thisPageNum * HOST_PAGE_SIZE;

        if (p > 0 && dirtyPages.at(p - 1) == thisPageNum - 1) {
            // Previous page was also dirty, update previous region
            regions.back().data =
              memView.subspan(regions.back().offset,
                              regions.back().data.size() + HOST_PAGE_SIZE);
        } else {
            // Previous page wasn't dirty, add new region
            regions.emplace_back(
              thisPageStart, memView.subspan(thisPageStart, HOST_PAGE_SIZE));
        }
    }

    return regions;
}

void SoftPTEDirtyTracker::reinitialise() {}

// ------------------------------
// Segfaults
// ------------------------------

class ThreadTrackingData
{
  public:
    ThreadTrackingData() = default;

    ThreadTrackingData(std::span<uint8_t> region)
      : regionBase(region.data())
      , regionTop(region.data() + region.size())
      , nPages(faabric::util::getRequiredHostPages(region.size()))
    {
        pageFlags = std::vector<bool>(nPages, false);
    }

    void markDirtyPage(void* addr)
    {
        ptrdiff_t offset = ((uint8_t*)addr) - regionBase;
        long pageNum = offset / HOST_PAGE_SIZE;
        pageFlags[pageNum] = true;

        SPDLOG_TRACE("Segfault offset {}, dirty page: {}", offset, pageNum);
    }

    std::vector<OffsetMemoryRegion> getDirtyRegions()
    {
        if (regionBase == nullptr) {
            return {};
        }

        std::span<uint8_t> regionView(regionBase, nPages * HOST_PAGE_SIZE);
        std::vector<OffsetMemoryRegion> dirty;
        for (int i = 0; i < nPages; i++) {
            if (!pageFlags.at(i)) {
                continue;
            }

            size_t thisRegionStart = i * HOST_PAGE_SIZE;

            // If last page was dirty, just update last region
            if (i > 0 && pageFlags.at(i - 1)) {
                dirty.back().data =
                  regionView.subspan(dirty.back().offset,
                                     dirty.back().data.size() + HOST_PAGE_SIZE);
            } else if (pageFlags.at(i)) {
                dirty.emplace_back(
                  thisRegionStart,
                  regionView.subspan(thisRegionStart, HOST_PAGE_SIZE));
            }
        }

        return dirty;
    }

    bool isInitialised() { return regionTop != nullptr; }

  private:
    uint8_t* regionBase = nullptr;
    uint8_t* regionTop = nullptr;
    int nPages = 0;
    std::vector<bool> pageFlags;
};

static thread_local ThreadTrackingData tracking;

SegfaultDirtyTracker::SegfaultDirtyTracker()
{
    setUpSignalHandler();
}

void SegfaultDirtyTracker::setUpSignalHandler()
{
    // Set up sig handler
    struct sigaction sa;
    sa.sa_flags = SA_SIGINFO;

    sigemptyset(&sa.sa_mask);
    sigaddset(&sa.sa_mask, SIGSEGV);

    sa.sa_sigaction = SegfaultDirtyTracker::handler;
    if (sigaction(SIGSEGV, &sa, NULL) == -1) {
        throw std::runtime_error("Failed sigaction");
    }

    SPDLOG_TRACE("Set up dirty tracking signal handler");
}

void SegfaultDirtyTracker::handler(int sig, siginfo_t* info, void* ucontext)
{
    void* faultAddr = info->si_addr;

    if (!tracking.isInitialised()) {
        SPDLOG_ERROR("Unexpected segfault, reraising");
        faabric::util::setUpCrashHandler(SIGSEGV);
        raise(sig);
    }

    tracking.markDirtyPage(faultAddr);

    // Align down to nearest page boundary
    uintptr_t addr = (uintptr_t)faultAddr;
    addr &= -HOST_PAGE_SIZE;
    auto* alignedAddr = (void*)addr;

    // Remove write protection from page
    if (::mprotect(alignedAddr, HOST_PAGE_SIZE, PROT_READ | PROT_WRITE) == -1) {
        SPDLOG_ERROR("WARNING: mprotect failed to unset read-only");
    }
}

void SegfaultDirtyTracker::clearAll()
{
    tracking = ThreadTrackingData();
}

void SegfaultDirtyTracker::startThreadLocalTracking(std::span<uint8_t> region)
{
    SPDLOG_TRACE("Starting thread-local tracking on region size {}",
                 region.size());
    tracking = ThreadTrackingData(region);
}

void SegfaultDirtyTracker::startTracking(std::span<uint8_t> region)
{
    SPDLOG_TRACE("Starting tracking on region size {}", region.size());

    // Note that here we want to mark the memory read-only, this is to ensure
    // that only writes are counted as dirtying a page.
    if (::mprotect(region.data(), region.size(), PROT_READ) == -1) {
        SPDLOG_ERROR("Failed to start tracking with mprotect: {} ({})",
                     errno,
                     strerror(errno));
        throw std::runtime_error("Failed mprotect to start tracking");
    }
}

void SegfaultDirtyTracker::stopTracking(std::span<uint8_t> region)
{
    if (region.empty()) {
        return;
    }

    SPDLOG_TRACE("Stopping tracking on region size {}", region.size());

    if (::mprotect(region.data(), region.size(), PROT_READ | PROT_WRITE) ==
        -1) {
        SPDLOG_ERROR("Failed to stop tracking with mprotect: {} ({})",
                     errno,
                     strerror(errno));
        throw std::runtime_error("Failed mprotect to stop tracking");
    }
}

void SegfaultDirtyTracker::stopThreadLocalTracking(std::span<uint8_t> region)
{
    // Do nothing - need to preserve thread-local data for getting dirty regions
    SPDLOG_TRACE("Stopping thread-local tracking on region size {}",
                 region.size());
}

void SegfaultDirtyTracker::reinitialise()
{
    if (faabric::util::isTestMode()) {
        // This is a hack because catch changes the segfault signal handler
        // between test cases, so we have to reinisiatlise
        setUpSignalHandler();
    }
}

std::vector<OffsetMemoryRegion>
SegfaultDirtyTracker::getThreadLocalDirtyOffsets(std::span<uint8_t> region)
{
    return tracking.getDirtyRegions();
}

std::vector<OffsetMemoryRegion> SegfaultDirtyTracker::getDirtyOffsets(
  std::span<uint8_t> region)
{
    return {};
}

std::vector<OffsetMemoryRegion> SegfaultDirtyTracker::getBothDirtyOffsets(
  std::span<uint8_t> region)
{
    return getThreadLocalDirtyOffsets(region);
}
}
