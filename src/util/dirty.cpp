#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <inttypes.h>
#include <memory>
#include <poll.h>
#include <shared_mutex>
#include <signal.h>
#include <sys/eventfd.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <thread>
#include <unistd.h>
#include <vector>

#include <faabric/util/crash.h>
#include <faabric/util/dirty.h>
#include <faabric/util/locks.h>
#include <faabric/util/memory.h>
#include <faabric/util/testing.h>
#include <faabric/util/timing.h>
#include <faabric/util/userfaultfd.h>

namespace faabric::util {

/**
 * Wrapper around the actual bookkeeping behind dirty tracking. This is
 * deliberately not thread-safe as it will only ever be used in TLS for signal
 * handlers.
 */
class DirtyTrackingRecord
{
  public:
    DirtyTrackingRecord() = default;

    virtual void setSourceMapping(std::span<uint8_t> source)
    {
        sourceMapping = source.data();
    }

    virtual void trackRegion(std::span<uint8_t> region)
    {
        nPages = faabric::util::getRequiredHostPages(region.size());
        dirtyFlags = std::vector<char>(nPages, 0);
        pagedFlags = std::vector<char>(nPages, 0);
        regionBase = region.data();
        regionTop = region.data() + region.size();
    }

    long getPage(void* addr)
    {
        return ((uint8_t*)addr - regionBase) / HOST_PAGE_SIZE;
    }

    virtual void markPageDirty(void* addr)
    {
        long pageNum = getPage(addr);
        markPageDirtyNum(pageNum);
    }

    bool isPaged(long pageNum) { return pagedFlags[pageNum] == 1; }

    void markPagePagedNum(long pageNum) { pagedFlags[pageNum] = 1; }

    void markPageDirtyNum(long pageNum) { dirtyFlags[pageNum] = 1; }

    virtual bool isInitialised() { return regionTop != nullptr; }

    virtual int getNPages() { return nPages; }

    virtual std::vector<char> getDirtyFlags() { return dirtyFlags; }

    virtual void reset()
    {
        nPages = 0;
        dirtyFlags.clear();
        pagedFlags.clear();

        sourceMapping = nullptr;
        regionBase = nullptr;
        regionTop = nullptr;
    }

    uint8_t* getRegionBase() { return regionBase; }

    uint8_t* getSourceMapping() { return sourceMapping; }

  protected:
    int nPages = 0;

    // std::vector<bool> here seems to worsen performance by >4x
    // std::vector<char> seems to be optimal
    std::vector<char> dirtyFlags;
    std::vector<char> pagedFlags;

    uint8_t* sourceMapping = nullptr;
    uint8_t* regionBase = nullptr;
    uint8_t* regionTop = nullptr;
};

/**
 * Thread-safe wrapper around dirty tracking data, necessary for use with the
 * background event thread in the userfaultfd tracker. Although the logic around
 * the interaction between the event thread and the main thread should in theory
 * protect against concurrent accesses, we need this to keep TSan happy.
 */
class ThreadSafeDirtyTrackingRecord final : public DirtyTrackingRecord
{
  public:
    ThreadSafeDirtyTrackingRecord() = default;

    void trackRegion(std::span<uint8_t> region) override
    {
        FullLock lock(mx);
        DirtyTrackingRecord::trackRegion(region);
    }

    void markPageDirty(void* addr) override
    {
        FullLock lock(mx);
        DirtyTrackingRecord::markPageDirty(addr);
    }

    std::vector<char> getDirtyFlags() override
    {
        SharedLock lock(mx);
        return DirtyTrackingRecord::getDirtyFlags();
    }

    bool isInitialised() override
    {
        SharedLock lock(mx);
        return DirtyTrackingRecord::isInitialised();
    }

    int getNPages() override
    {
        SharedLock lock(mx);
        return DirtyTrackingRecord::getNPages();
    }

    void reset() override
    {
        FullLock lock(mx);
        DirtyTrackingRecord::reset();
    }

  private:
    std::shared_mutex mx;
};

void* pageAlignAddress(void* faultAddr)
{
    uintptr_t addr = (uintptr_t)faultAddr;
    addr &= -HOST_PAGE_SIZE;
    auto* alignedAddr = (void*)addr;
    return alignedAddr;
}

// Thread-local tracking information for dirty tracking using signal
// handlers in the same thread as the fault.
static thread_local DirtyTrackingRecord tracking;

// Global tracking information is used for non-thread-local tracking
static ThreadSafeDirtyTrackingRecord globalTracking;

static std::shared_ptr<DirtyTracker> tracker = nullptr;

void resetDirtyTracker()
{
    tracker = nullptr;

    std::string mode = getSystemConfig().dirtyTrackingMode;

    if (mode == "softpte") {
        tracker = std::make_shared<SoftPTEDirtyTracker>(mode);
    } else if (mode == "segfault") {
        tracker = std::make_shared<SegfaultDirtyTracker>(mode);
    } else if (mode == "none") {
        tracker = std::make_shared<NoneDirtyTracker>(mode);
    } else if (mode == "uffd" || mode == "uffd-demand" || mode == "uffd-wp" ||
               mode == "uffd-thread" || mode == "uffd-thread-wp") {
        tracker = std::make_shared<UffdDirtyTracker>(mode);
    } else {
        SPDLOG_ERROR("Unrecognised dirty tracking mode: {}", mode);
        throw std::runtime_error("Unrecognised dirty tracking mode");
    }
}

std::shared_ptr<DirtyTracker> getDirtyTracker()
{
    if (tracker == nullptr) {
        resetDirtyTracker();
    }

    return tracker;
}

// ----------------------------------
// Soft dirty PTE
// ----------------------------------

SoftPTEDirtyTracker::SoftPTEDirtyTracker(const std::string& modeIn)
  : DirtyTracker(modeIn)
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

    // Disable buffering, we want to repeatedly read updates to the same
    // file
    setbuf(pagemapFile, nullptr);
}

SoftPTEDirtyTracker::~SoftPTEDirtyTracker()
{
    ::fclose(clearRefsFile);
    ::fclose(pagemapFile);
}

void SoftPTEDirtyTracker::mapRegions(std::span<uint8_t> source,
                                     std::span<uint8_t> dest)
{
    throw std::runtime_error("Mapping not supported");
}

void SoftPTEDirtyTracker::clearAll()
{
    resetPTEs();
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
    // Do nothing
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

SegfaultDirtyTracker::SegfaultDirtyTracker(const std::string& modeIn)
  : DirtyTracker(modeIn)
{
    // Sigaction docs:
    // https://www.man7.org/linux/man-pages/man2/sigaction.2.html
    struct sigaction sa;
    sa.sa_flags = SA_SIGINFO | SA_NODEFER;

    sigemptyset(&sa.sa_mask);
    sigaddset(&sa.sa_mask, SIGSEGV);

    sa.sa_sigaction = SegfaultDirtyTracker::handler;
    if (sigaction(SIGSEGV, &sa, nullptr) != 0) {
        throw std::runtime_error("Failed sigaction");
    }

    SPDLOG_TRACE("Set up dirty tracking SIGSEGV handler");
}

void SegfaultDirtyTracker::mapRegions(std::span<uint8_t> source,
                                      std::span<uint8_t> dest)
{
    throw std::runtime_error("Mapping not supported");
}

void SegfaultDirtyTracker::clearAll()
{
    tracking.reset();
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

    tracking.markPageDirty(faultAddr);

    // Align down to nearest page boundary
    void* alignedAddr = pageAlignAddress(faultAddr);

    // Remove write protection from page
    if (::mprotect(alignedAddr, HOST_PAGE_SIZE, PROT_READ | PROT_WRITE) != 0) {
        SPDLOG_ERROR("WARNING: mprotect failed to unset read-only");
    }
}

void SegfaultDirtyTracker::startThreadLocalTracking(std::span<uint8_t> region)
{
    if (region.empty() || region.data() == nullptr) {
        SPDLOG_WARN("Empty region passed, not starting thread-local tracking");
        return;
    }

    SPDLOG_TRACE("Starting thread-local tracking on region size {}",
                 region.size());
    tracking.trackRegion(region);
}

void SegfaultDirtyTracker::startTracking(std::span<uint8_t> region)
{
    SPDLOG_TRACE("Starting tracking on region size {}", region.size());

    if (region.empty() || region.data() == nullptr) {
        SPDLOG_WARN("Empty region passed, not starting tracking");
        return;
    }

    PROF_START(MprotectStart)

    // Note that here we want to mark the memory read-only, this is to
    // ensure that only writes are counted as dirtying a page.
    if (::mprotect(region.data(), region.size(), PROT_READ) != 0) {
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
        SPDLOG_WARN("Empty region passed, not stopping tracking");
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
    // Do nothing - need to preserve thread-local data for getting dirty
    // regions
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

    return tracking.getDirtyFlags();
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

// Shared uffd file descriptor when using userfaultfd
static long uffd = -1;

// Global flags
static bool uffdWriteProtect = false;
static bool uffdSigbus = false;
static bool uffdDemandPaging = false;

static int closeFd = -1;
static std::shared_ptr<std::thread> eventThread = nullptr;

UffdDirtyTracker::UffdDirtyTracker(const std::string& modeIn)
  : DirtyTracker(modeIn)
{
    if (mode == "uffd") {
        writeProtect = false;
        sigbus = true;
        demandPaging = false;
    } else if (mode == "uffd-demand") {
        writeProtect = true;
        sigbus = true;
        demandPaging = true;
    } else if (mode == "uffd-wp") {
        writeProtect = true;
        sigbus = true;
        demandPaging = false;
    } else if (mode == "uffd-thread") {
        writeProtect = false;
        sigbus = false;
        demandPaging = false;
    } else if (mode == "uffd-thread-wp") {
        writeProtect = true;
        sigbus = false;
        demandPaging = false;
    }

    SPDLOG_DEBUG("Uffd dirty tracking: write-protect={}, sigbus={}, demand={}",
                 writeProtect,
                 sigbus,
                 demandPaging);

    // Set up global flags
    uffdWriteProtect = writeProtect;
    uffdSigbus = sigbus;
    uffdDemandPaging = demandPaging;

    if (uffdSigbus) {
        // Set up the sigbus handler
        struct sigaction sa;
        sa.sa_flags = SA_SIGINFO;
        sa.sa_handler = nullptr;
        sa.sa_sigaction = UffdDirtyTracker::sigbusHandler;
        sigemptyset(&sa.sa_mask);

        if (sigaction(SIGBUS, &sa, nullptr) != 0) {
            SPDLOG_ERROR("Failed to set up SIGBUS handler: {} ({})",
                         errno,
                         strerror(errno));
            throw std::runtime_error("Failed to set up SIGBUS");
        }

        SPDLOG_TRACE("Set up dirty tracking SIGBUS handler");
    }

    // Initialise uffd
    initUffd();
}

void UffdDirtyTracker::initUffd()
{
    // Set up userfaultfd
    uffd = syscall(__NR_userfaultfd, O_NONBLOCK);
    if (uffd == -1) {
        SPDLOG_ERROR("Failed on userfaultfd: {} ({})", errno, strerror(errno));
        throw std::runtime_error("userfaultfd failed");
    }

    // Check API features
    __u64 features = 0;
    if (uffdSigbus) {
        features |= UFFD_FEATURE_SIGBUS;
    }

    struct uffdio_api uffdApi = { .api = UFFD_API, .features = features };

    if (::ioctl(uffd, UFFDIO_API, &uffdApi) != 0) {
        SPDLOG_ERROR("Failed on ioctl API {} ({})", errno, strerror(errno));
        throw std::runtime_error("Userfaultfd API failed");
    }

    bool shmemSupported = uffdApi.features & UFFD_FEATURE_MISSING_SHMEM;
    if (!shmemSupported) {
        SPDLOG_ERROR("Userfaultfd shared memory not supported");
        throw std::runtime_error("Userfaultfd shared memory not supported");
    }

    if (uffdWriteProtect) {
        bool writeProtectSupported =
          uffdApi.features & UFFD_FEATURE_PAGEFAULT_FLAG_WP;
        if (!writeProtectSupported) {
            SPDLOG_ERROR("Userfaultfd write protect not supported");
            throw std::runtime_error("Userfaultfd write protect not supported");
        }
    }

    if (!uffdSigbus) {
        // Open shutdown fd for closing event thread
        closeFd = ::eventfd(0, 0);
        if (closeFd == -1) {
            SPDLOG_ERROR(
              "Failed to open eventfd for closing uffd event thread");
            throw std::runtime_error("Failed to open eventfd");
        }

        // Start event thread
        eventThread = std::make_shared<std::thread>(
          &UffdDirtyTracker::eventThreadEntrypoint);
    }

    SPDLOG_DEBUG("Initialised uffd {} (sigbus {}, write-protect {})",
                 uffd,
                 uffdSigbus,
                 uffdWriteProtect);
}

void UffdDirtyTracker::stopUffd()
{
    if (!uffdSigbus && eventThread != nullptr) {
        SPDLOG_DEBUG("Sending shutdown to event thread on {}", closeFd);

        // This message can be anything, so its value doesn't matter.
        uint64_t msg = 1;
        ::write(closeFd, &msg, sizeof(uint64_t));

        if (eventThread->joinable()) {
            eventThread->join();
        }

        eventThread = nullptr;

        ::close(closeFd);
        closeFd = -1;
    }

    // Close down the uffd handle
    if (uffd > 0) {
        SPDLOG_DEBUG("Closing uffd {}", uffd);
        ::close(uffd);
        uffd = -1;
    }
}

void UffdDirtyTracker::mapRegions(std::span<uint8_t> source,
                                  std::span<uint8_t> dest)
{
    if (source.size() != dest.size()) {
        SPDLOG_ERROR("Source and dest not equal size: {} != {}",
                     source.size(),
                     dest.size());
        throw std::runtime_error(
          "Source and destination mapping not equal size");
    }

    tracking.setSourceMapping(source);
}

UffdDirtyTracker::~UffdDirtyTracker()
{
    stopUffd();
}

void UffdDirtyTracker::eventThreadEntrypoint()
{
    SPDLOG_TRACE(
      "Starting uffd event thread (uffd={}, closeFd={})", uffd, closeFd);
    int nFds = 2;
    struct pollfd pollfds[nFds];

    pollfds[0].fd = uffd;
    pollfds[0].events = POLLIN;

    pollfds[1].fd = closeFd;
    pollfds[1].events = POLLIN;

    for (;;) {
        int nReady = poll(pollfds, nFds, -1);
        if (nReady == -1) {
            SPDLOG_ERROR("Poll failed: {} ({})", errno, strerror(errno));
            throw std::runtime_error("Poll failed");
        }

        if (pollfds[0].revents & POLLERR) {
            SPDLOG_DEBUG("Ignoring uffd POLLERR");
            continue;
        }

        if ((pollfds[1].revents & POLLERR) || (pollfds[1].revents & POLLIN)) {
            SPDLOG_DEBUG("Received uffd shut down");
            return;
        }

        // Read an event
        uffd_msg msg;
        ssize_t nRead = read(uffd, &msg, sizeof(msg));
        if (nRead == 0) {
            SPDLOG_ERROR("EOF on userfaultfd: {} ({})", errno, strerror(errno));
            throw std::runtime_error("EOF on uffd");
        }

        if (nRead == -1) {
            SPDLOG_ERROR("Read failed: {} ({})", errno, strerror(errno));
            throw std::runtime_error("Read failed");
        }

        if (msg.event != UFFD_EVENT_PAGEFAULT) {
            SPDLOG_ERROR("Unexpected userfault event: {}", msg.event);
            throw std::runtime_error("Unexpected userfault event");
        }

        // Get page-aligned address
        void* faultAddr = (void*)msg.arg.pagefault.address;
        void* alignedAddr = pageAlignAddress(faultAddr);

        // Events will ALWAYS have UFFD_PAGEFAULT_FLAG_WRITE set, but will
        // only have UFFD_PAGEFAULT_FLAG_WP set when it's a write-protected
        // event
        bool isWriteEvent = msg.arg.pagefault.flags & UFFD_PAGEFAULT_FLAG_WRITE;
        bool isWriteProtected =
          msg.arg.pagefault.flags & UFFD_PAGEFAULT_FLAG_WP;

        // Mark the page dirty if there's been a write
        if (isWriteEvent) {
            globalTracking.markPageDirty(faultAddr);
        }

        if (isWriteProtected) {
            SPDLOG_TRACE("Uffd thread got write on write-protected page {}",
                         __u64(alignedAddr));

            removeWriteProtectFromPage((uint8_t*)alignedAddr, true);
        } else {
            SPDLOG_TRACE("Uffd thread got missing page event at {} (write={})",
                         __u64(alignedAddr),
                         isWriteEvent);

            zeroPage((uint8_t*)alignedAddr);
        }
    }
}

void UffdDirtyTracker::clearAll()
{
    tracking.reset();
    globalTracking.reset();
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

    if (uffdDemandPaging) {
        long pageNum = tracking.getPage(faultAddr);
        long offset = pageNum * HOST_PAGE_SIZE;

        // If page is not yet paged in, we need to copy from the source to the
        // dest. If it is already paged in, this is a write on the paged region,
        // so we need to remove write protection and mark it.
        if (tracking.isPaged(pageNum)) {
            // Remove write-protection
            removeWriteProtectFromPage(tracking.getRegionBase() + offset,
                                       false);

            // Mark page (can safely do this after thread has woken up as this
            // will only ever be thread-local)
            tracking.markPageDirtyNum(pageNum);
        } else {
            // Copy source page into dest page
            copyPage(tracking.getSourceMapping() + offset,
                     tracking.getRegionBase() + offset);

            tracking.markPagePagedNum(pageNum);
        }
    } else {
        // Mark the page
        tracking.markPageDirty(faultAddr);

        // Get page-aligned address
        void* alignedAddr = pageAlignAddress(faultAddr);

        bool exists = zeroPage((uint8_t*)alignedAddr);

        if (exists) {
            removeWriteProtectFromPage((uint8_t*)alignedAddr, false);
        }
    }
}

void UffdDirtyTracker::startThreadLocalTracking(std::span<uint8_t> region)
{
    if (region.empty() || region.data() == nullptr) {
        SPDLOG_WARN("Empty region passed, not starting thread-local tracking");
        return;
    }

    SPDLOG_TRACE("Starting thread-local tracking on region {} {}",
                 (__u64)region.data(),
                 region.size());

    tracking.trackRegion(region);
}

void UffdDirtyTracker::startTracking(std::span<uint8_t> region)
{
    SPDLOG_TRACE("Starting tracking on region size {}", region.size());

    if (region.empty() || region.data() == nullptr) {
        SPDLOG_WARN("Empty region passed, not starting tracking");
        return;
    }

    globalTracking.trackRegion(region);

    registerRegion(region);

    if (writeProtect) {
        writeProtectRegion(region);
    }
}

void UffdDirtyTracker::registerRegion(std::span<uint8_t> region)
{
    // Register the range
    struct uffdio_range regRange = { .start = (__u64)region.data(),
                                     .len = region.size() };

    __u64 mode = UFFDIO_REGISTER_MODE_MISSING;

    // Register for write-protect events if necessary
    if (uffdWriteProtect) {
        mode |= UFFDIO_REGISTER_MODE_WP;
    }

    struct uffdio_register uffdRegister = { .range = regRange, .mode = mode };

    int res = ::ioctl(uffd, UFFDIO_REGISTER, &uffdRegister);
    if (res != 0) {
        SPDLOG_ERROR(
          "Failed to register range: {} ({})", errno, strerror(errno));
        throw std::runtime_error("Range register failed");
    }

    SPDLOG_TRACE("Uffd registered {} {}", regRange.start, regRange.len);
}

void UffdDirtyTracker::writeProtectRegion(std::span<uint8_t> region)
{
    struct uffdio_range wpRange = { .start = (__u64)region.data(),
                                    .len = region.size() };
    struct uffdio_writeprotect wpArgs = { .range = wpRange,
                                          .mode = UFFDIO_WRITEPROTECT_MODE_WP };

    if (::ioctl(uffd, UFFDIO_WRITEPROTECT, &wpArgs) != 0) {
        SPDLOG_ERROR(
          "Failed to write-protect range: {} ({})", errno, strerror(errno));
        throw std::runtime_error("Write protect failed");
    }

    SPDLOG_TRACE("Uffd write-protected {} {}", wpRange.start, wpRange.len);
}

void UffdDirtyTracker::stopTracking(std::span<uint8_t> region)
{
    if (region.empty() || region.data() == nullptr) {
        SPDLOG_WARN("Empty region passed, not stopping tracking");
        return;
    }

    SPDLOG_TRACE("Stopping tracking on region size {}", region.size());

    if (writeProtect) {
        removeWriteProtectFromPage(region.data(), true);
    }

    deregisterRegion(region);
}

void UffdDirtyTracker::copyPage(uint8_t* source, uint8_t* dest)
{
    // Note that here we also set the copied page to write-protected, this way
    // we can track writes on the newly copied page
    struct uffdio_copy copyDef
    {
        .dst = (__u64)dest, .src = (__u64)source, .len = (__u64)HOST_PAGE_SIZE,
        .mode = UFFDIO_COPY_MODE_WP
    };

    if (::ioctl(uffd, UFFDIO_COPY, &copyDef) != 0) {
        SPDLOG_ERROR("Failed to copy range: {} ({})", errno, strerror(errno));
        throw std::runtime_error("Range copy");
    }
}

bool UffdDirtyTracker::zeroPage(uint8_t* region)
{
    struct uffdio_range zeroRange = { .start = (__u64)region,
                                      .len = (__u64)HOST_PAGE_SIZE };
    uffdio_zeropage zeroPage = { .range = zeroRange, .mode = 0 };

    int result = ::ioctl(uffd, UFFDIO_ZEROPAGE, &zeroPage);

    return result != 0;
}

void UffdDirtyTracker::deregisterRegion(std::span<uint8_t> region)
{
    unsigned int ioctls = 0;
    struct uffdio_range deregRange = { .start = (__u64)region.data(),
                                       .len = region.size() };
    struct uffdio_register deregister = { .range = deregRange,
                                          .mode = 0,
                                          .ioctls = ioctls };

    if (::ioctl(uffd, UFFDIO_UNREGISTER, &deregister) != 0) {
        SPDLOG_ERROR(
          "Failed to deregister range: {} ({})", errno, strerror(errno));
        throw std::runtime_error("Range deregister failed");
    }
}

void UffdDirtyTracker::removeWriteProtectFromPage(uint8_t* region, bool throwEx)
{
    struct uffdio_range uffdRange
    {
        .start = (__u64)region, .len = (__u64)HOST_PAGE_SIZE
    };
    struct uffdio_writeprotect wpArgs = { uffdRange, 0 };

    if (::ioctl(uffd, UFFDIO_WRITEPROTECT, &wpArgs) != 0) {
        SPDLOG_ERROR(
          "Failed removing write protection: {} ({})", errno, strerror(errno));

        if (throwEx) {
            throw std::runtime_error("Failed removing write protection");
        }

        exit(1);
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

    return tracking.getDirtyFlags();
}

std::vector<char> UffdDirtyTracker::getDirtyPages(std::span<uint8_t> region)
{
    if (sigbus) {
        return {};
    }

    if (!globalTracking.isInitialised()) {
        size_t nPages = getRequiredHostPages(region.size());
        return std::vector<char>(nPages, 0);
    }

    return globalTracking.getDirtyFlags();
}

std::vector<char> UffdDirtyTracker::getBothDirtyPages(std::span<uint8_t> region)
{
    if (sigbus) {
        return getThreadLocalDirtyPages(region);
    }

    return getDirtyPages(region);
}

// ------------------------------
// None (i.e. mark all pages dirty)
// ------------------------------

NoneDirtyTracker::NoneDirtyTracker(const std::string& modeIn)
  : DirtyTracker(modeIn)
{}

void NoneDirtyTracker::mapRegions(std::span<uint8_t> source,
                                  std::span<uint8_t> dest)
{
    throw std::runtime_error("Map regions not supported");
}

void NoneDirtyTracker::clearAll()
{
    dirtyPages.clear();
}

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
