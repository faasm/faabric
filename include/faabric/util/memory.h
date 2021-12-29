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
// Dirty pages
// -------------------------
void resetDirtyTracking();

std::vector<int> getDirtyPageNumbers(const uint8_t* ptr, int nPages);

std::vector<std::pair<uint32_t, uint32_t>> getDirtyRegions(const uint8_t* ptr,
                                                           int nPages);
// -------------------------
// Dirty tracking alternatives
// -------------------------

class MprotectRegionTracker
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

    std::vector<std::pair<uint32_t, uint32_t>> getDirty() { return dirty; }

    void stop() {}

  private:
    std::vector<std::pair<uint32_t, uint32_t>> dirty;
    std::span<uint8_t> region;
};

class UffdRegionTracker
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
