#pragma once

#include <signal.h>
#include <span>
#include <string>

#include <faabric/util/config.h>
#include <faabric/util/logging.h>
#include <faabric/util/memory.h>

#define CLEAR_REFS "/proc/self/clear_refs"
#define PAGEMAP "/proc/self/pagemap"

#define PAGEMAP_ENTRY_BYTES sizeof(uint64_t)
#define PAGEMAP_SOFT_DIRTY (1Ull << 55)

namespace faabric::util {

/*
 * Interface to all dirty page tracking. Available types and implementation
 * details in classes below.
 */
class DirtyTracker
{
  public:
    virtual void clearAll() = 0;

    virtual std::string getType() = 0;

    virtual void startTracking(std::span<uint8_t> region) = 0;

    virtual void stopTracking(std::span<uint8_t> region) = 0;

    virtual std::vector<char> getDirtyPages(std::span<uint8_t> region) = 0;

    virtual void startThreadLocalTracking(std::span<uint8_t> region) = 0;

    virtual void stopThreadLocalTracking(std::span<uint8_t> region) = 0;

    virtual std::vector<char> getThreadLocalDirtyPages(
      std::span<uint8_t> region) = 0;

    virtual std::vector<char> getBothDirtyPages(std::span<uint8_t> region) = 0;
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

    void clearAll() override;

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
};

/*
 * Dirty tracking implementation using mprotect to make pages read-only and
 * use segfaults resulting from writes to mark them as dirty.
 */
class SegfaultDirtyTracker final : public DirtyTracker
{
  public:
    SegfaultDirtyTracker();

    void clearAll() override;

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

  private:
    void setUpSignalHandler();
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

    void clearAll() override;

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

DirtyTracker& getDirtyTracker();
}
