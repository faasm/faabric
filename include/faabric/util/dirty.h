#pragma once

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

namespace faabric::util {

/*
 * Interface to all dirty page tracking. Available types and implementation
 * details in classes below.
 */
class DirtyTracker
{
  public:
    DirtyTracker(const std::string& modeIn)
      : mode(modeIn)
    {}

    virtual void clearAll() = 0;

    virtual std::string getType() = 0;

    virtual void startTracking(std::span<uint8_t> region) = 0;

    virtual void stopTracking(std::span<uint8_t> region) = 0;

    virtual std::vector<char> getDirtyPages(std::span<uint8_t> region) = 0;

    virtual void startThreadLocalTracking(std::span<uint8_t> region) = 0;

    virtual void stopThreadLocalTracking(std::span<uint8_t> region) = 0;

    virtual void mapRegions(std::span<uint8_t> source, std::span<uint8_t> dest) = 0;

    virtual std::vector<char> getThreadLocalDirtyPages(
      std::span<uint8_t> region) = 0;

    virtual std::vector<char> getBothDirtyPages(std::span<uint8_t> region) = 0;

  protected:
    const std::string mode;
};

/*
 * Dirty tracking implementation using soft-dirty PTEs
 * https://www.kernel.org/doc/html/latest/admin-guide/mm/soft-dirty.html
 */
class SoftPTEDirtyTracker final : public DirtyTracker
{
  public:
    SoftPTEDirtyTracker(const std::string& modeIn);

    ~SoftPTEDirtyTracker();

    void clearAll() override;

    std::string getType() override { return "softpte"; }

    void startTracking(std::span<uint8_t> region) override;

    void stopTracking(std::span<uint8_t> region) override;

    std::vector<char> getDirtyPages(std::span<uint8_t> region) override;

    void startThreadLocalTracking(std::span<uint8_t> region) override;

    void stopThreadLocalTracking(std::span<uint8_t> region) override;

    void mapRegions(std::span<uint8_t> source, std::span<uint8_t> dest) override;

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
    SegfaultDirtyTracker(const std::string& modeIn);

    void clearAll() override;

    std::string getType() override { return "segfault"; }

    void startTracking(std::span<uint8_t> region) override;

    void stopTracking(std::span<uint8_t> region) override;

    std::vector<char> getDirtyPages(std::span<uint8_t> region) override;

    void startThreadLocalTracking(std::span<uint8_t> region) override;

    void stopThreadLocalTracking(std::span<uint8_t> region) override;

    void mapRegions(std::span<uint8_t> source, std::span<uint8_t> dest) override;

    std::vector<char> getThreadLocalDirtyPages(
      std::span<uint8_t> region) override;

    std::vector<char> getBothDirtyPages(std::span<uint8_t> region) override;

    // Signal handler for the resulting segfaults
    static void handler(int sig, siginfo_t* info, void* ucontext) noexcept;
};

/**
 * Dirty tracking implementation using userfaultfd to write-protect pages, then
 * handle the resulting userspace events when they are written to.
 *
 * The dirty tracking mode can be one of four options:
 *
 * - uffd - uses the `SIGBUS` handler to catch events triggered by accessing
 *   missing pages in demand-zero paged memory.
 * - uffd-wp - same as `uffd` but adds write-protected events to catch
 *   subsequent writes to write-protected pages.
 * - uffd-thread - same as `uffd`, but using a background event thread to handle
 *   events. This has the benefit of distinguishing between read and write
 *   missing page events.
 * - uffd-thread-wp - same as `uffd-thread`, but adds write-protected events.
 *
 * See the docs for more info on these different approaches:
 * https://www.kernel.org/doc/html/latest/admin-guide/mm/userfaultfd.html
 */
class UffdDirtyTracker final : public DirtyTracker
{
  public:
    UffdDirtyTracker(const std::string& modeIn);

    ~UffdDirtyTracker();

    void clearAll() override;

    std::string getType() override { return mode; }

    void startTracking(std::span<uint8_t> region) override;

    void stopTracking(std::span<uint8_t> region) override;

    std::vector<char> getDirtyPages(std::span<uint8_t> region) override;

    void startThreadLocalTracking(std::span<uint8_t> region) override;

    void stopThreadLocalTracking(std::span<uint8_t> region) override;

    void mapRegions(std::span<uint8_t> source, std::span<uint8_t> dest) override;

    std::vector<char> getThreadLocalDirtyPages(
      std::span<uint8_t> region) override;

    std::vector<char> getBothDirtyPages(std::span<uint8_t> region) override;

    static void sigbusHandler(int sig,
                              siginfo_t* info,
                              void* ucontext) noexcept;

  private:
    bool writeProtect = false;

    bool sigbus = false;

    bool demandPaging = false;

    static void initUffd();

    static void stopUffd();

    static void registerRegion(std::span<uint8_t> region);

    static void writeProtectRegion(std::span<uint8_t> region);

    static void removeWriteProtectFromPage(uint8_t* region, bool throwEx);

    static void copyPage(uint8_t* source, uint8_t* dest);

    static bool zeroPage(uint8_t* region);

    static void deregisterRegion(std::span<uint8_t> region);

    static void eventThreadEntrypoint();
};

/*
 * This tracker just marks all pages as dirty. This may be optimal for workloads
 * with a small memory where most of that memory will be dirty anyway, so
 * diffing every page outweighs the cost of the dirty tracking.
 */
class NoneDirtyTracker final : public DirtyTracker
{
  public:
    NoneDirtyTracker(const std::string& modeIn);

    void clearAll() override;

    std::string getType() override { return "none"; }

    void startTracking(std::span<uint8_t> region) override;

    void stopTracking(std::span<uint8_t> region) override;

    std::vector<char> getDirtyPages(std::span<uint8_t> region) override;

    void startThreadLocalTracking(std::span<uint8_t> region) override;

    void stopThreadLocalTracking(std::span<uint8_t> region) override;

    void mapRegions(std::span<uint8_t> source, std::span<uint8_t> dest) override;

    std::vector<char> getThreadLocalDirtyPages(
      std::span<uint8_t> region) override;

    std::vector<char> getBothDirtyPages(std::span<uint8_t> region) override;

  private:
    std::vector<char> dirtyPages;
};

/**
 * Returns the dirty tracker singleton. The dirty tracking mode is determined in
 * the system config.
 */
std::shared_ptr<DirtyTracker> getDirtyTracker();

/**
 * Resets the dirty tracker singleton (e.g. if the config has been changed).
 */
void resetDirtyTracker();
}
