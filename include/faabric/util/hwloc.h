#pragma once

#include <pthread.h>
#include <thread>

namespace faabric::util {

const int NO_CPU_IDX = -1;
const int GHA_CPU_IDX = -2;

class FaabricCpuSet
{
  public:
    FaabricCpuSet(int cpuIdxIn = NO_CPU_IDX);
    FaabricCpuSet& operator=(const FaabricCpuSet&) = delete;
    FaabricCpuSet(const FaabricCpuSet&) = delete;

    ~FaabricCpuSet();

    cpu_set_t* get() { return &cpuSet; }

  private:
    cpu_set_t cpuSet;

    // CPU index in internal CPU accounting
    int cpuIdx = NO_CPU_IDX;
};

// Pin thread to any "unpinned" CPUs. Returns the CPU set it was pinned to.
// We return a unique pointer to enforce RAII on the pinned-to CPU
std::unique_ptr<FaabricCpuSet> pinThreadToFreeCpu(pthread_t thread);

// Pin thread to a given CPU set. This can be used to pin two threads together
void pinThreadToCpu(pthread_t thread, cpu_set_t* cpuSet);
}
