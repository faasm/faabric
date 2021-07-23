#pragma once

#include <condition_variable>
#include <functional>
#include <mutex>

namespace faabric::util {

#define DEFAULT_BARRIER_TIMEOUT_MS 10000

class Barrier
{
  public:
    // WARNING: this barrier must be shared between threads using a shared
    // pointer, otherwise there seems to be some nasty race conditions related
    // to its destruction.
    static std::shared_ptr<Barrier> create(
      int count,
      std::function<void()> completionFunctionIn,
      int timeoutMs = DEFAULT_BARRIER_TIMEOUT_MS);

    static std::shared_ptr<Barrier> create(
      int count,
      int timeoutMs = DEFAULT_BARRIER_TIMEOUT_MS);

    explicit Barrier(int countIn,
                     std::function<void()> completionFunctionIn,
                     int timeoutMsIn);

    void wait();

  private:
    int count = 0;
    int visits = 0;
    int currentPhase = 1;

    std::function<void()> completionFunction;

    int timeoutMs;

    std::mutex mx;
    std::condition_variable cv;
};
}
