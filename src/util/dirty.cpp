#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <inttypes.h>
#include <linux/userfaultfd.h>
#include <memory>
#include <poll.h>
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
#include <faabric/util/timing.h>

namespace faabric::util {

DirtyTracker getDirtyTracker()
{
    std::string trackMode = faabric::util::getSystemConfig().dirtyTrackingMode;
    if (trackMode == "softpte") {
        return SoftPTEDirtyTracker();
    }

    if (trackMode == "segfault") {
        return SegfaultDirtyTracker();
    }

    if (trackMode == "uffd") {
        return UffdDirtyTracker();
    }

    if (trackMode == "none") {
        return NoneDirtyTracker();
    }

    SPDLOG_ERROR("Unrecognised dirty tracking mode: {}", trackMode);
    throw std::runtime_error("Unrecognised dirty tracking mode");
}

// ----------------------------------
// Dummy interface implementation
//
// This is required because we can't return an instance of a pure abstract class
// from a method.
// ----------------------------------

std::string DirtyTracker::getType()
{
    throw std::runtime_error("Not implemented");
}

void DirtyTracker::startTracking(std::span<uint8_t> region)
{
    throw std::runtime_error("Not implemented");
}

void DirtyTracker::stopTracking(std::span<uint8_t> region)
{
    throw std::runtime_error("Not implemented");
}

std::vector<char> DirtyTracker::getDirtyPages(std::span<uint8_t> region)
{
    throw std::runtime_error("Not implemented");
}

void DirtyTracker::startThreadLocalTracking(std::span<uint8_t> region)
{
    throw std::runtime_error("Not implemented");
}

void DirtyTracker::stopThreadLocalTracking(std::span<uint8_t> region)
{
    throw std::runtime_error("Not implemented");
}

std::vector<char> DirtyTracker::getThreadLocalDirtyPages(
  std::span<uint8_t> region)
{
    throw std::runtime_error("Not implemented");
}

std::vector<char> DirtyTracker::getBothDirtyPages(std::span<uint8_t> region)
{
    throw std::runtime_error("Not implemented");
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

    pagemapFile = ::fopen(PAGEMAP, "rb");
    if (pagemapFile == nullptr) {
        SPDLOG_ERROR("Could not open pagemap ({})", strerror(errno));
        throw std::runtime_error("Could not open pagemap");
    }

    // Disable buffering, we want to repeatedly read updates to the same file
    setbuf(pagemapFile, nullptr);
}

SoftPTEDirtyTracker::~SoftPTEDirtyTracker()
{
    ::fclose(clearRefsFile);
    ::fclose(pagemapFile);
}

void SoftPTEDirtyTracker::startTracking(std::span<uint8_t> region)
{
    resetPTEs();
}

void SoftPTEDirtyTracker::startThreadLocalTracking(std::span<uint8_t> region)
{
    // Do nothing
}

void SoftPTEDirtyTracker::stopTracking(std::span<uint8_t> region)
{
    resetPTEs();
}

void SoftPTEDirtyTracker::resetPTEs()
{
    PROF_START(ClearSoftPTE)
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
    PROF_END(ClearSoftPTE)
}

void SoftPTEDirtyTracker::stopThreadLocalTracking(std::span<uint8_t> region)
{
    // Do nothing
}

std::vector<char> SoftPTEDirtyTracker::getDirtyPages(std::span<uint8_t> region)
{
    PROF_START(GetDirtyRegions)

    int nPages = getRequiredHostPages(region.size());
    uint8_t* ptr = region.data();
    uintptr_t vptr = (uintptr_t)ptr;

    // Work out offset for this pointer in the pagemap
    off_t offset = (vptr / HOST_PAGE_SIZE) * PAGEMAP_ENTRY_BYTES;

    // Skip to location of this page (note that fd is open in binary mode)
    int r = ::fseek(pagemapFile, offset, 0);
    if (r != 0) {
        SPDLOG_ERROR("Could not seek pagemap to {}, returned {}", offset, r);
        throw std::runtime_error("Could not seek pagemap");
    }

    // Read the entries
    std::vector<uint64_t> entries(nPages, 0);
    int nRead =
      ::fread(entries.data(), PAGEMAP_ENTRY_BYTES, nPages, pagemapFile);
    if (nRead != nPages) {
        SPDLOG_ERROR("Could not read pagemap ({} != {})", nRead, nPages);
        throw std::runtime_error("Could not read pagemap");
    }

    // Iterate through the pagemap entries to work out which are dirty
    std::vector<char> regions(nPages, 0);
    for (int i = 0; i < nPages; i++) {
        bool isDirty = entries.at(i) & PAGEMAP_SOFT_DIRTY;
        if (isDirty) {
            regions[i] = 1;
        }
    }

    SPDLOG_TRACE(
      "Out of {} pages, found {} dirty regions", nPages, regions.size());

    PROF_END(GetDirtyRegions)
    return regions;
}

std::vector<char> SoftPTEDirtyTracker::getBothDirtyPages(
  std::span<uint8_t> region)
{
    return getDirtyPages(region);
}

std::vector<char> SoftPTEDirtyTracker::getThreadLocalDirtyPages(
  std::span<uint8_t> region)
{
    return {};
}

// ------------------------------
// Segfaults
// ------------------------------

class ThreadTrackingData
{
  public:
    ThreadTrackingData() = default;

    ThreadTrackingData(std::span<uint8_t> region)
      : nPages(faabric::util::getRequiredHostPages(region.size()))
      , pageFlags(nPages, 0)
      , regionBase(region.data())
      , regionTop(region.data() + region.size())
    {}

    void markDirtyPage(void* addr)
    {
        ptrdiff_t offset = ((uint8_t*)addr) - regionBase;
        long pageNum = offset / HOST_PAGE_SIZE;
        pageFlags[pageNum] = 1;
    }

    bool isInitialised() { return regionTop != nullptr; }

    // std::vector<bool> here seems to worsen performance by >4x
    // std::vector<char> seems to be optimal
    int nPages = 0;
    std::vector<char> pageFlags;

  private:
    uint8_t* regionBase = nullptr;
    uint8_t* regionTop = nullptr;
};

static thread_local ThreadTrackingData tracking;

SegfaultDirtyTracker::SegfaultDirtyTracker()
{
    // See sigaction docs
    // https://www.man7.org/linux/man-pages/man2/sigaction.2.html
    struct sigaction sa;
    sa.sa_flags = SA_SIGINFO | SA_NODEFER;

    sigemptyset(&sa.sa_mask);
    sigaddset(&sa.sa_mask, SIGSEGV);

    sa.sa_sigaction = SegfaultDirtyTracker::handler;
    if (sigaction(SIGSEGV, &sa, NULL) == -1) {
        throw std::runtime_error("Failed sigaction");
    }

    SPDLOG_TRACE("Set up dirty tracking SIGSEGV handler");
}

void SegfaultDirtyTracker::handler(int sig,
                                   siginfo_t* info,
                                   void* ucontext) noexcept
{
    void* faultAddr = info->si_addr;

    if (!tracking.isInitialised()) {
        // Unexpected segfault, treat as normal
        faabric::util::handleCrash(sig);
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

void SegfaultDirtyTracker::startThreadLocalTracking(std::span<uint8_t> region)
{
    if (region.empty() || region.data() == nullptr) {
        return;
    }

    SPDLOG_TRACE("Starting thread-local tracking on region size {}",
                 region.size());
    tracking = ThreadTrackingData(region);
}

void SegfaultDirtyTracker::startTracking(std::span<uint8_t> region)
{
    SPDLOG_TRACE("Starting tracking on region size {}", region.size());

    if (region.empty() || region.data() == nullptr) {
        return;
    }

    PROF_START(MprotectStart)

    // Note that here we want to mark the memory read-only, this is to ensure
    // that only writes are counted as dirtying a page.
    if (::mprotect(region.data(), region.size(), PROT_READ) == -1) {
        SPDLOG_ERROR("Failed to start tracking with mprotect: {} ({})",
                     errno,
                     strerror(errno));
        throw std::runtime_error("Failed mprotect to start tracking");
    }

    PROF_END(MprotectStart)
}

void SegfaultDirtyTracker::stopTracking(std::span<uint8_t> region)
{
    if (region.empty() || region.data() == nullptr) {
        return;
    }

    SPDLOG_TRACE("Stopping tracking on region size {}", region.size());

    PROF_START(MprotectEnd)

    if (::mprotect(region.data(), region.size(), PROT_READ | PROT_WRITE) ==
        -1) {
        SPDLOG_ERROR("Failed to stop tracking with mprotect: {} ({})",
                     errno,
                     strerror(errno));
        throw std::runtime_error("Failed mprotect to stop tracking");
    }

    PROF_END(MprotectEnd)
}

void SegfaultDirtyTracker::stopThreadLocalTracking(std::span<uint8_t> region)
{
    // Do nothing - need to preserve thread-local data for getting dirty regions
    SPDLOG_TRACE("Stopping thread-local tracking on region size {}",
                 region.size());
}

std::vector<char> SegfaultDirtyTracker::getThreadLocalDirtyPages(
  std::span<uint8_t> region)
{
    if (!tracking.isInitialised()) {
        size_t nPages = getRequiredHostPages(region.size());
        return std::vector<char>(nPages, 0);
    }

    return tracking.pageFlags;
}

std::vector<char> SegfaultDirtyTracker::getDirtyPages(std::span<uint8_t> region)
{
    return {};
}

std::vector<char> SegfaultDirtyTracker::getBothDirtyPages(
  std::span<uint8_t> region)
{
    return getThreadLocalDirtyPages(region);
}

// ------------------------------
// Userfaultfd
// ------------------------------

UffdDirtyTracker::UffdDirtyTracker()
{
    // Set up userfaultfd
    uffd = syscall(__NR_userfaultfd, O_CLOEXEC | O_NONBLOCK);
    if (uffd == -1) {
        SPDLOG_ERROR("Failed on userfaultfd: {} ({})", errno, strerror(errno));
        throw std::runtime_error("userfaultfd failed");
    }

    // Check API features
    struct uffdio_api uffdApi = { .api = UFFD_API,
                                  .features = UFFD_FEATURE_SIGBUS };
    if (ioctl(uffd, UFFDIO_API, &uffdApi) == -1) {
        SPDLOG_ERROR("Failed on ioctl API {} ({})", errno, strerror(errno));
        throw std::runtime_error("ioctl API failed");
    }

    bool writeProtectSupported =
      uffdApi.features & UFFD_FEATURE_PAGEFAULT_FLAG_WP;
    if (!writeProtectSupported) {
        SPDLOG_ERROR("Userfaultfd write protect not supported");
        throw std::runtime_error("Userfaultfd write protect not supported");
    }

    struct sigaction sa;
    sa.sa_flags = SA_SIGINFO | SA_RESTART;

    sigemptyset(&sa.sa_mask);
    sigaddset(&sa.sa_mask, SIGBUS);

    sa.sa_sigaction = UffdDirtyTracker::sigbusHandler;

    if (sigaction(SIGBUS, &sa, nullptr) != 0) {
        SPDLOG_ERROR(
          "Failed to set up SIGBUS handler: {} ({})", errno, strerror(errno));
        throw std::runtime_error("Failed to set up SIGBUS");
    }

    SPDLOG_TRACE("Set up dirty tracking SIGBUS handler");
}

void UffdDirtyTracker::sigbusHandler(int sig,
                                     siginfo_t* info,
                                     void* ucontext) noexcept
{
    void* faultAddr = info->si_addr;

    if (!tracking.isInitialised()) {
        // Unexpected sigbus fault, treat as normal
        faabric::util::handleCrash(sig);
    }

    tracking.markDirtyPage(faultAddr);

    // Remove write protection from page
    uintptr_t addr = (uintptr_t)faultAddr;
    addr &= -HOST_PAGE_SIZE;
    auto* alignedAddr = (void*)addr;

    struct uffdio_range uffdRange = { (uint64_t)alignedAddr,
                                      (size_t)HOST_PAGE_SIZE };
    struct uffdio_writeprotect wpArgs = { uffdRange, 0 };

    if (ioctl(uffd, UFFDIO_WRITEPROTECT, &wpArgs) == -1) {
        SPDLOG_ERROR(
          "Failed removing write protection: {} ({})", errno, strerror(errno));

        throw std::runtime_error("Failed removing write protection");
    }
}

void UffdDirtyTracker::startThreadLocalTracking(std::span<uint8_t> region)
{
    if (region.empty() || region.data() == nullptr) {
        return;
    }

    SPDLOG_TRACE("Starting thread-local tracking on region size {}",
                 region.size());

    tracking = ThreadTrackingData(region);
}

void UffdDirtyTracker::startTracking(std::span<uint8_t> region)
{
    SPDLOG_TRACE("Starting tracking on region size {}", region.size());

    if (region.empty() || region.data() == nullptr) {
        return;
    }

    struct uffdio_range uffdRange = { (uint64_t)region.data(), region.size() };
    struct uffdio_register uffdRegister = { uffdRange,
                                            UFFDIO_REGISTER_MODE_WP,
                                            (uint32_t)0 };

    // Register the range
    if (ioctl(uffd, UFFDIO_REGISTER, &uffdRegister) == -1) {
        SPDLOG_ERROR(
          "Failed to register range: {} ({})", errno, strerror(errno));
        throw std::runtime_error("Range register failed");
    }

    // Write protect it
    struct uffdio_writeprotect uffdArgs = { uffdRange,
                                            UFFDIO_WRITEPROTECT_MODE_WP };

    if (ioctl(uffd, UFFDIO_WRITEPROTECT, &uffdArgs) == -1) {
        SPDLOG_ERROR(
          "Failed to write-protect range: {} ({})", errno, strerror(errno));
        throw std::runtime_error("Write protect failed");
    }
}

void UffdDirtyTracker::stopTracking(std::span<uint8_t> region)
{
    if (region.empty() || region.data() == nullptr) {
        return;
    }

    SPDLOG_TRACE("Stopping tracking on region size {}", region.size());
    struct uffdio_range uffdRange = { (uint64_t)region.data(), region.size() };
    struct uffdio_writeprotect wpArgs = { uffdRange, 0 };

    if (ioctl(uffd, UFFDIO_WRITEPROTECT, &wpArgs) == -1) {
        SPDLOG_ERROR(
          "Failed removing write protection: {} ({})", errno, strerror(errno));

        throw std::runtime_error("Failed removing write protection");
    }
}

void UffdDirtyTracker::stopThreadLocalTracking(std::span<uint8_t> region)
{
    SPDLOG_TRACE("Stopping thread-local tracking on region size {}",
                 region.size());
}

std::vector<char> UffdDirtyTracker::getThreadLocalDirtyPages(
  std::span<uint8_t> region)
{
    if (!tracking.isInitialised()) {
        size_t nPages = getRequiredHostPages(region.size());
        return std::vector<char>(nPages, 0);
    }

    return tracking.pageFlags;
}

std::vector<char> UffdDirtyTracker::getDirtyPages(std::span<uint8_t> region)
{
    return {};
}

std::vector<char> UffdDirtyTracker::getBothDirtyPages(std::span<uint8_t> region)
{
    return getThreadLocalDirtyPages(region);
}

// ------------------------------
// None (i.e. mark all pages dirty)
// ------------------------------

void NoneDirtyTracker::startThreadLocalTracking(std::span<uint8_t> region) {}

void NoneDirtyTracker::startTracking(std::span<uint8_t> region)
{
    size_t nPages = getRequiredHostPages(region.size());
    dirtyPages = std::vector<char>(nPages, 1);
}

void NoneDirtyTracker::stopTracking(std::span<uint8_t> region) {}

void NoneDirtyTracker::stopThreadLocalTracking(std::span<uint8_t> region) {}

std::vector<char> NoneDirtyTracker::getThreadLocalDirtyPages(
  std::span<uint8_t> region)
{
    return {};
}

std::vector<char> NoneDirtyTracker::getDirtyPages(std::span<uint8_t> region)
{
    return dirtyPages;
}

std::vector<char> NoneDirtyTracker::getBothDirtyPages(std::span<uint8_t> region)
{
    return getDirtyPages(region);
}
}
