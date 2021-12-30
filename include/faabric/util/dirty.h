#pragma once

#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <functional>
#include <inttypes.h>
#include <linux/userfaultfd.h>
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

#include <faabric/util/config.h>
#include <faabric/util/logging.h>
#include <faabric/util/memory.h>

#define CLEAR_REFS "/proc/self/clear_refs"
#define PAGEMAP "/proc/self/pagemap"

#define PAGEMAP_ENTRY_BYTES 8
#define PAGEMAP_SOFT_DIRTY (1Ull << 55)

namespace faabric::util {

enum SnapshotDataType
{
    Raw,
    Bool,
    Int,
    Long,
    Float,
    Double
};

enum SnapshotMergeOperation
{
    Overwrite,
    Sum,
    Product,
    Subtract,
    Max,
    Min,
    Ignore
};

class SnapshotDiff
{
  public:
    SnapshotDiff() = default;

    SnapshotDiff(SnapshotDataType dataTypeIn,
                 SnapshotMergeOperation operationIn,
                 uint32_t offsetIn,
                 std::span<const uint8_t> dataIn);

    SnapshotDataType getDataType() const { return dataType; }

    SnapshotMergeOperation getOperation() const { return operation; }

    uint32_t getOffset() const { return offset; }

    std::span<const uint8_t> getData() const { return data; }

    std::vector<uint8_t> getDataCopy() const;

  private:
    SnapshotDataType dataType = SnapshotDataType::Raw;
    SnapshotMergeOperation operation = SnapshotMergeOperation::Overwrite;
    uint32_t offset = 0;
    std::vector<uint8_t> data;
};

class DirtyPageTracker
{
  public:
    void restartTracking(std::span<uint8_t> region)
    {
        stopTracking(region);
        startTracking(region);
    }

    virtual std::vector<SnapshotDiff> getDirty(std::span<uint8_t> region);

    virtual std::vector<std::pair<uint32_t, uint32_t>> getDirtyOffsets(
      std::span<uint8_t> region);

    virtual void clearAll() { dirtyRegions.clear(); }

    virtual void startTracking(std::span<uint8_t> region);

    virtual void stopTracking(std::span<uint8_t> region);

  protected:
    std::vector<std::pair<uint32_t, uint32_t>> dirtyRegions;

    std::vector<std::pair<uint32_t, uint32_t>> dirtyLookup(
      std::span<uint8_t> region)
    {
        std::vector<std::pair<uint32_t, uint32_t>> found;

        // TODO look up regions in dirty list

        return found;
    }

    void markRegion(uint32_t start, uint32_t end)
    {
        dirtyRegions.emplace_back(start, end);
    }
};

class SoftPTEDirtyTracker : public DirtyPageTracker
{
  public:
    SoftPTEDirtyTracker()
    {

        f = ::fopen(CLEAR_REFS, "w");
        if (f == nullptr) {
            SPDLOG_ERROR("Could not open clear_refs ({})", strerror(errno));
            throw std::runtime_error("Could not open clear_refs");
        }
    }

    ~SoftPTEDirtyTracker() { ::fclose(f); }

    void startTracking(std::span<uint8_t> region) override
    {
        // Do nothing
    }

    void stopTracking(std::span<uint8_t> region) override
    {
        // Write 4 to the file to track from now on
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

    std::vector<std::pair<uint32_t, uint32_t>> getDirtyOffsets(
      std::span<uint8_t> region) override
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

    std::vector<SnapshotDiff> getDirty(std::span<uint8_t> region) override
    {
        std::vector<std::pair<uint32_t, uint32_t>> regions =
          getDirtyOffsets(region);

        // Convert to snapshot diffs
        std::vector<SnapshotDiff> diffs;
        diffs.reserve(regions.size());
        for (auto [regionBegin, regionEnd] : regions) {
            SPDLOG_TRACE("Memory view dirty {}-{}", regionBegin, regionEnd);
            diffs.emplace_back(
              SnapshotDataType::Raw,
              SnapshotMergeOperation::Overwrite,
              regionBegin,
              region.subspan(regionBegin, regionEnd - regionBegin));
        }

        return diffs;
    }

  private:
    FILE* f = nullptr;

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

    std::vector<std::pair<uint32_t, uint32_t>> getDirtyRegions(
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
};

class MprotectRegionTracker : public DirtyPageTracker
{
  public:
    MprotectRegionTracker() = default;

    static std::vector<std::pair<uint32_t, uint32_t>> dirty;

    static void handler(int sig, siginfo_t* si, void* unused)
    {
        // TODO - work out the page that's dirtied

        // TODO - register dirty page somewhere

        // TODO - reset mprotect to READ/WRITE
    }

    void start(std::span<uint8_t> regionIn)
    {
        region = regionIn;

        struct sigaction sa;

        sa.sa_flags = SA_SIGINFO;
        sigemptyset(&sa.sa_mask);
        sa.sa_sigaction = handler;
        if (sigaction(SIGSEGV, &sa, NULL) == -1) {
            throw std::runtime_error("Failed sigaction");
        }

        if (::mprotect(region.data(), region.size(), PROT_READ) == -1) {
            throw std::runtime_error("Failed mprotect");
        }
    }

    std::vector<std::pair<uint32_t, uint32_t>> getDirty()
    {
        std::vector<std::pair<uint32_t, uint32_t>> dirty;
        return dirty;
    }

    void stop() {}

  private:
    std::span<uint8_t> region;
};

class UffdRegionTracker : public DirtyPageTracker
{
  public:
    UffdRegionTracker() = default;

    void start(std::span<const uint8_t> regionIn) { start(regionIn, -1); }

    void start(std::span<const uint8_t> regionIn, int fd)
    {
        region = regionIn;

        // Create uffd
        uffd = syscall(__NR_userfaultfd, O_CLOEXEC | O_NONBLOCK);
        if (uffd == -1) {
            SPDLOG_ERROR(
              "Failed on userfaultfd: {} ({})", errno, strerror(errno));
            throw std::runtime_error("userfaultfd failed");
        }

        // Check uffd API
        struct uffdio_api uffdApi;
        uffdApi.api = UFFD_API;
        uffdApi.features = UFFD_FEATURE_EVENT_UNMAP | UFFD_FEATURE_EVENT_REMAP |
                           UFFD_FEATURE_MISSING_SHMEM |
                           UFFD_FEATURE_EVENT_REMOVE;
        if (ioctl(uffd, UFFDIO_API, &uffdApi) == -1) {
            SPDLOG_ERROR("Failed on ioctl API {} ({})", errno, strerror(errno));
            throw std::runtime_error("ioctl API failed");
        }

        // Register uffd
        struct uffdio_register uffdRegister;
        uffdRegister.range.start = (unsigned long)region.data();
        uffdRegister.range.len = region.size();
        uffdRegister.mode = UFFDIO_REGISTER_MODE_MISSING;
        if (ioctl(uffd, UFFDIO_REGISTER, &uffdRegister) == -1) {
            SPDLOG_ERROR(
              "Failed on ioctl register {} ({})", errno, strerror(errno));
            throw std::runtime_error("ioctl register failed");
        }

        // Thread to monitor for events
        trackerThread = std::thread([this, fd] {
            for (;;) {
                // Poll for events
                struct pollfd pollfd;
                int nready;
                pollfd.fd = uffd;
                pollfd.events = POLLIN;
                nready = poll(&pollfd, 1, -1);
                if (nready == -1) {
                    SPDLOG_ERROR("Poll failed");
                    throw std::runtime_error("Poll failed");
                }

                // Read an event
                uffd_msg msg;
                ssize_t nread = read(uffd, &msg, sizeof(msg));
                if (nread == 0) {
                    throw std::runtime_error("EOF while reading userfaultfd");
                }

                if (nread == -1) {
                    throw std::runtime_error("Reading userfaultfd failed");
                }

                if (msg.event == UFFD_EVENT_UNMAP) {
                    SPDLOG_TRACE("Memory unmapped, finishing tracking");
                    break;
                }

                if (msg.event != UFFD_EVENT_PAGEFAULT) {
                    SPDLOG_ERROR("Unexpected userfault event: {}", msg.event);
                    throw std::runtime_error("Unexpected userfault event");
                }

                size_t pageBase =
                  ((uint8_t*)msg.arg.pagefault.address) - region.data();

                bool isDirty = false;
                bool zero = false;
                if (msg.arg.pagefault.flags == 0) {
                    SPDLOG_TRACE("Uffd read: {} ({})",
                                 msg.arg.pagefault.address,
                                 pageBase);
                    zero = true;
                } else if (msg.arg.pagefault.flags &
                           UFFD_PAGEFAULT_FLAG_WRITE) {
                    SPDLOG_TRACE("Uffd write: {} ({})",
                                 msg.arg.pagefault.address,
                                 pageBase);

                    isDirty = true;
                    zero = fd <= 0;
                } else {
                    SPDLOG_ERROR("Unexpected pagefault flag: {}",
                                 msg.arg.pagefault.flags);
                    throw std::runtime_error("Pagefault flag not as expected");
                }

                if (isDirty) {
                    // Record that this page is now dirty
                    dirty.emplace_back(pageBase, pageBase + HOST_PAGE_SIZE);
                }

                // Resolve the page fault so that the waiting thread can
                // continue. If the fault was triggered from a standard private
                // memory mapping, this means we can just write a zero page. If
                // it was caused by a copy-on-write mapping, we need to copy the
                // source page into the destination.
                if (zero) {
                    uffdio_zeropage zeroPage;
                    zeroPage.range.start = msg.arg.pagefault.address;
                    zeroPage.range.len = HOST_PAGE_SIZE;

                    int ioctlRes = ioctl(uffd, UFFDIO_ZEROPAGE, &zeroPage);

                    if (ioctlRes == -1) {
                        SPDLOG_ERROR("ioctl zeropage failed {} {}",
                                     errno,
                                     strerror(errno));
                    }
                } else {
                    uffdio_copy copyPage;

                    copyPage.src = msg.arg.pagefault.address;
                    copyPage.dst = (unsigned long)msg.arg.pagefault.address &
                                   ~(HOST_PAGE_SIZE - 1);
                    copyPage.len = HOST_PAGE_SIZE;
                    copyPage.mode = 0;
                    copyPage.copy = 0;

                    int ioctlRes = ioctl(uffd, UFFDIO_COPY, &copyPage);

                    if (ioctlRes == -1) {
                        SPDLOG_ERROR("ioctl copypage failed {} {}",
                                     errno,
                                     strerror(errno));
                    }
                }
            }
        });
    }

    std::vector<std::pair<uint32_t, uint32_t>> getDirty() { return dirty; }

    void stop()
    {
        if (trackerThread.joinable()) {
            trackerThread.join();
        }
    }

  private:
    std::vector<std::pair<uint32_t, uint32_t>> dirty;
    std::span<const uint8_t> region;
    long uffd;

    std::thread trackerThread;
};

DirtyPageTracker& getDirtyPageTracker()
{
    static SoftPTEDirtyTracker spte;
    static MprotectRegionTracker mprot;
    static UffdRegionTracker uffd;

    std::string trackMode = faabric::util::getSystemConfig().dirtyTrackingMode;
    if (trackMode == "softpte") {
        return spte;
    } else if (trackMode == "sigseg") {
        return mprot;
    } else if (trackMode == "uffd") {
        return uffd;
    }
}
}
