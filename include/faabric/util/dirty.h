#pragma once

#include <linux/userfaultfd.h>
#include <signal.h>
#include <span>
#include <string>
#include <thread>

#include <faabric/util/config.h>
#include <faabric/util/logging.h>
#include <faabric/util/memory.h>

#define CLEAR_REFS "/proc/self/clear_refs"
#define PAGEMAP "/proc/self/pagemap"

#define PAGEMAP_ENTRY_BYTES sizeof(uint64_t)
#define PAGEMAP_SOFT_DIRTY (1Ull << 55)

// The following isn't available using HWE on 20.04, so copied from main at:
// https://github.com/torvalds/linux/blob/master/fs/userfaultfd.c

#define _UFFDIO_WRITEPROTECT (0x06)
#define UFFDIO_WRITEPROTECT                                                    \
    _IOWR(UFFDIO, _UFFDIO_WRITEPROTECT, struct uffdio_writeprotect)

struct uffdio_writeprotect
{
    struct uffdio_range range;
#define UFFDIO_WRITEPROTECT_MODE_WP ((__u64)1 << 0)
#define UFFDIO_WRITEPROTECT_MODE_DONTWAKE ((__u64)1 << 1)
    __u64 mode;
};

namespace faabric::util {

/*
 * Interface to all dirty page tracking. Available types and implementation
 * details in classes below.
 */
class DirtyTracker
{
  public:
    DirtyTracker() = default;

    virtual std::string getType();

    virtual void startTracking(std::span<uint8_t> region);

    virtual void stopTracking(std::span<uint8_t> region);

    virtual std::vector<char> getDirtyPages(std::span<uint8_t> region);

    virtual void startThreadLocalTracking(std::span<uint8_t> region);

    virtual void stopThreadLocalTracking(std::span<uint8_t> region);

    virtual std::vector<char> getThreadLocalDirtyPages(
      std::span<uint8_t> region);

    virtual std::vector<char> getBothDirtyPages(std::span<uint8_t> region);
};

/*
 * Dirty tracking implementation using soft-dirty PTEs
 * https://www.kernel.org/doc/html/latest/admin-guide/mm/soft-dirty.html
 */
class SoftPTEDirtyTracker final : public DirtyTracker
{
  public:
    SoftPTEDirtyTracker();

    ~SoftPTEDirtyTracker();

    std::string getType() override { return "softpte"; }

    void startTracking(std::span<uint8_t> region) override;

    void stopTracking(std::span<uint8_t> region) override;

    std::vector<char> getDirtyPages(std::span<uint8_t> region) override;

    void startThreadLocalTracking(std::span<uint8_t> region) override;

    void stopThreadLocalTracking(std::span<uint8_t> region) override;

    std::vector<char> getThreadLocalDirtyPages(
      std::span<uint8_t> region) override;

    std::vector<char> getBothDirtyPages(std::span<uint8_t> region) override;

  private:
    FILE* clearRefsFile = nullptr;

    FILE* pagemapFile = nullptr;

    void resetPTEs();
};

/*
 * Dirty tracking implementation using mprotect to make pages read-only and
 * use segfaults resulting from writes to mark them as dirty.
 */
class SegfaultDirtyTracker final : public DirtyTracker
{
  public:
    SegfaultDirtyTracker();

    std::string getType() override { return "segfault"; }

    void startTracking(std::span<uint8_t> region) override;

    void stopTracking(std::span<uint8_t> region) override;

    std::vector<char> getDirtyPages(std::span<uint8_t> region) override;

    void startThreadLocalTracking(std::span<uint8_t> region) override;

    void stopThreadLocalTracking(std::span<uint8_t> region) override;

    std::vector<char> getThreadLocalDirtyPages(
      std::span<uint8_t> region) override;

    std::vector<char> getBothDirtyPages(std::span<uint8_t> region) override;

    // Signal handler for the resulting segfaults
    static void handler(int sig, siginfo_t* info, void* ucontext) noexcept;
};

/*
 * Dirty tracking implementation using userfaultfd to write-protect pages, then
 * handle the resulting userspace events when they are written to.
 */
class UffdDirtyTracker final : public DirtyTracker
{
  public:
    UffdDirtyTracker();

    std::string getType() override { return "uffd"; }

    void startTracking(std::span<uint8_t> region) override;

    void stopTracking(std::span<uint8_t> region) override;

    std::vector<char> getDirtyPages(std::span<uint8_t> region) override;

    void startThreadLocalTracking(std::span<uint8_t> region) override;

    void stopThreadLocalTracking(std::span<uint8_t> region) override;

    std::vector<char> getThreadLocalDirtyPages(
      std::span<uint8_t> region) override;

    std::vector<char> getBothDirtyPages(std::span<uint8_t> region) override;

    static void sigbusHandler(int sig, siginfo_t* info, void* ucontext) noexcept;

  private:
    static long uffd;
};

/*
 * This tracker just marks all pages as dirty. This may be optimal for workloads
 * with a small memory where most of that memory will be dirty anyway, so
 * diffing every page outweighs the cost of the dirty tracking.
 */
class NoneDirtyTracker final : public DirtyTracker
{
  public:
    NoneDirtyTracker() = default;

    std::string getType() override { return "none"; }

    void startTracking(std::span<uint8_t> region) override;

    void stopTracking(std::span<uint8_t> region) override;

    std::vector<char> getDirtyPages(std::span<uint8_t> region) override;

    void startThreadLocalTracking(std::span<uint8_t> region) override;

    void stopThreadLocalTracking(std::span<uint8_t> region) override;

    std::vector<char> getThreadLocalDirtyPages(
      std::span<uint8_t> region) override;

    std::vector<char> getBothDirtyPages(std::span<uint8_t> region) override;

  private:
    std::vector<char> dirtyPages;
};

/**
 * Factory method to create a dirty tracker instance based on the tracking mode
 * set in the configuration.
 */
DirtyTracker getDirtyTracker();
}
