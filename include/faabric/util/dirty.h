#pragma once

#include <signal.h>
#include <span>
#include <string>

#include <faabric/util/config.h>
#include <faabric/util/logging.h>
#include <faabric/util/memory.h>

#define CLEAR_REFS "/proc/self/clear_refs"
#define PAGEMAP "/proc/self/pagemap"

#define PAGEMAP_ENTRY_BYTES 8
#define PAGEMAP_SOFT_DIRTY (1Ull << 55)

namespace faabric::util {

class DirtyTracker
{
  public:
    virtual void clearAll() = 0;

    virtual void reinitialise() = 0;

    virtual void startTracking(std::span<uint8_t> region) = 0;

    virtual void stopTracking(std::span<uint8_t> region) = 0;

    virtual std::vector<OffsetMemoryRegion> getDirtyOffsets(
      std::span<uint8_t> region) = 0;

    virtual void startThreadLocalTracking(std::span<uint8_t> region) = 0;

    virtual void stopThreadLocalTracking(std::span<uint8_t> region) = 0;

    virtual std::vector<OffsetMemoryRegion> getThreadLocalDirtyOffsets(
      std::span<uint8_t> region) = 0;

    virtual std::vector<OffsetMemoryRegion> getBothDirtyOffsets(
      std::span<uint8_t> region) = 0;
};

class SoftPTEDirtyTracker final : public DirtyTracker
{
  public:
    SoftPTEDirtyTracker();

    ~SoftPTEDirtyTracker();

    void clearAll() override;

    void reinitialise() override;

    void startTracking(std::span<uint8_t> region) override;

    void stopTracking(std::span<uint8_t> region) override;

    std::vector<OffsetMemoryRegion> getDirtyOffsets(
      std::span<uint8_t> region) override;

    void startThreadLocalTracking(std::span<uint8_t> region) override;

    void stopThreadLocalTracking(std::span<uint8_t> region) override;

    std::vector<OffsetMemoryRegion> getThreadLocalDirtyOffsets(
      std::span<uint8_t> region) override;

    std::vector<OffsetMemoryRegion> getBothDirtyOffsets(
      std::span<uint8_t> region) override;

  private:
    FILE* clearRefsFile = nullptr;

    std::vector<uint64_t> readPagemapEntries(uintptr_t ptr, int nEntries);

    std::vector<int> getDirtyPageNumbers(const uint8_t* ptr, int nPages);

    std::vector<OffsetMemoryRegion> getDirtyRegions(uint8_t* ptr, int nPages);
};

class SegfaultDirtyTracker final : public DirtyTracker
{
  public:
    SegfaultDirtyTracker();

    void clearAll() override;

    void reinitialise() override;

    void startTracking(std::span<uint8_t> region) override;

    void stopTracking(std::span<uint8_t> region) override;

    std::vector<OffsetMemoryRegion> getDirtyOffsets(
      std::span<uint8_t> region) override;

    void startThreadLocalTracking(std::span<uint8_t> region) override;

    void stopThreadLocalTracking(std::span<uint8_t> region) override;

    std::vector<OffsetMemoryRegion> getThreadLocalDirtyOffsets(
      std::span<uint8_t> region) override;

    std::vector<OffsetMemoryRegion> getBothDirtyOffsets(
      std::span<uint8_t> region) override;

    static void handler(int sig, siginfo_t* si, void* unused);

  private:
    void setUpSignalHandler();
};

DirtyTracker& getDirtyTracker();
}
