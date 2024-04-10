#include <faabric/util/config.h>
#include <faabric/util/hwloc.h>
#include <faabric/util/logging.h>
#include <faabric/util/testing.h>

#include <atomic>
#include <cassert>
#include <thread>
#include <vector>

namespace faabric::util {

// Work-around not being able to define and default-initialise an array of
// atomics
class FreeCpus
{
  public:
    FreeCpus()
    {
        for (int i = 0; i < std::jthread::hardware_concurrency(); i++) {
            cpuVec.emplace_back(std::make_unique<std::atomic<bool>>(true));
        }
    }

    std::vector<std::unique_ptr<std::atomic<bool>>> cpuVec;
};
static FreeCpus freeCpus;

FaabricCpuSet::FaabricCpuSet(int cpuIdxIn)
  : cpuIdx(cpuIdxIn)
{
    CPU_ZERO(&cpuSet);

    // Populate the CPU set struct
    if (cpuIdx != NO_CPU_IDX && cpuIdx != GHA_CPU_IDX) {
        assert(cpuIdx >= 0);
        assert(cpuIdx < std::jthread::hardware_concurrency());

        CPU_SET(cpuIdx, &cpuSet);
    } else if (cpuIdx == GHA_CPU_IDX) {
        // If overcommitting on GHA, pin to zero
        CPU_SET(0, &cpuSet);
    }
}

FaabricCpuSet::~FaabricCpuSet()
{
    // Free the occupied CPU
    if (cpuIdx != NO_CPU_IDX && cpuIdx != GHA_CPU_IDX) {
        assert(cpuIdx >= 0);
        assert(cpuIdx < std::jthread::hardware_concurrency());

        [[maybe_unused]] bool oldVal = std::atomic_exchange_explicit(
          freeCpus.cpuVec.at(cpuIdx).get(), true, std::memory_order_acquire);
        // Check that the value held was not free (i.e. false)
        assert(!oldVal);
    }

    CPU_ZERO(&cpuSet);
};

// A free CPU means a CPU that has not been pinned to using pinThreadToFreeCpu
static std::unique_ptr<FaabricCpuSet> getNextFreeCpu()
{
    int startIdx =
      std::max<int>(0, faabric::util::getSystemConfig().overrideFreeCpuStart);
    for (int i = startIdx; i < freeCpus.cpuVec.size(); i++) {
        // Stop when we find a true value. Otherwise we are just swapping false
        // and false, so we don't change the real occupation
        if (std::atomic_exchange_explicit(
              freeCpus.cpuVec.at(i).get(), false, std::memory_order_acquire)) {
            return std::make_unique<FaabricCpuSet>(i);
        }
    }

    // In test mode, we allow running out of CPUs when pinning to support
    // OVERRIDE_CPU_COUNT-like mechanisms to run the tests in constrained
    // environments like GHA. If we have run out of CPUs, and we are in test
    // mode, we return a special cpu index
    if (isTestMode()) {
        return std::make_unique<FaabricCpuSet>(GHA_CPU_IDX);
    }

    SPDLOG_ERROR("Ran-out of free CPU cores to pin to! (total cores: {})",
                 std::jthread::hardware_concurrency());
    throw std::runtime_error("Ran-out of free CPU cores!");
}

static void doPinThreadToCpu(pthread_t thread, cpu_set_t* cpuSet)
{
    int errCode = pthread_setaffinity_np(thread, sizeof(cpu_set_t), cpuSet);
    if (errCode != 0) {
        SPDLOG_ERROR("Error setting thread affinity: {} (code: {})",
                     strerror(errCode),
                     errCode);
        throw std::runtime_error("Error setting thread affinity!");
    }
}

std::unique_ptr<FaabricCpuSet> pinThreadToFreeCpu(pthread_t thread)
{
    // First, get a "free" CPU
    std::unique_ptr<FaabricCpuSet> cpuSet = getNextFreeCpu();

    // Then, pin the caller thread to this cpu set
    doPinThreadToCpu(thread, cpuSet->get());

    return cpuSet;
}
}
