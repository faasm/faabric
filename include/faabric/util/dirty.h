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

class DirtyPageTracker
{
  public:
    virtual void restartTracking(std::span<uint8_t> region) = 0;

    virtual std::vector<std::pair<uint32_t, uint32_t>> getDirtyOffsets(
      std::span<uint8_t> region) = 0;

    virtual void clearAll() = 0;

    virtual void startTracking(std::span<uint8_t> region) = 0;

    virtual void stopTracking(std::span<uint8_t> region) = 0;
};

class SoftPTEDirtyTracker final : public DirtyPageTracker
{
  public:
    SoftPTEDirtyTracker();

    ~SoftPTEDirtyTracker();

    void clearAll() override;

    void restartTracking(std::span<uint8_t> region) override;

    void startTracking(std::span<uint8_t> region) override;

    void stopTracking(std::span<uint8_t> region) override;

    std::vector<std::pair<uint32_t, uint32_t>> getDirtyOffsets(
      std::span<uint8_t> region) override;

  private:
    FILE* f = nullptr;

    std::vector<uint64_t> readPagemapEntries(uintptr_t ptr, int nEntries);

    std::vector<int> getDirtyPageNumbers(const uint8_t* ptr, int nPages);

    std::vector<std::pair<uint32_t, uint32_t>> getDirtyRegions(
      const uint8_t* ptr,
      int nPages);
};

class MprotectRegionTracker final : public DirtyPageTracker
{
  public:
    MprotectRegionTracker();

    void clearAll() override;

    void restartTracking(std::span<uint8_t> region) override;

    void startTracking(std::span<uint8_t> region) override;

    void stopTracking(std::span<uint8_t> region) override;

    std::vector<std::pair<uint32_t, uint32_t>> getDirtyOffsets(
      std::span<uint8_t> region) override;

    static void handler(int sig, siginfo_t* si, void* unused);

    void stop() {}
};

DirtyPageTracker& getDirtyPageTracker();
}
