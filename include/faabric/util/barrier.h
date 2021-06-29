#pragma once

#include <condition_variable>
#include <mutex>

namespace faabric::util {

#define DEFAULT_BARRIER_TIMEOUT_MS 10000

class Barrier
{
  public:
    explicit Barrier(int count, int timeoutMsIn = DEFAULT_BARRIER_TIMEOUT_MS);

    void wait();

    int getSlotCount();

    int getUseCount();

  private:
    int threadCount;
    int slotCount;
    int uses;
    int timeoutMs;

    std::mutex mx;
    std::condition_variable cv;
};
}
