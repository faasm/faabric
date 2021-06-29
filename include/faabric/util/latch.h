#pragma once

#include <condition_variable>
#include <mutex>

namespace faabric::util {

#define DEFAULT_BARRIER_TIMEOUT_MS 10000

class Latch
{
  public:
    explicit Latch(int countIn, int timeoutMsIn = DEFAULT_BARRIER_TIMEOUT_MS);

    void wait();
  private:
    int count;
    int waiters = 0;

    int timeoutMs;

    std::mutex mx;
    std::condition_variable cv;
};
}
