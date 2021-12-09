#pragma once

#include <condition_variable>
#include <memory>
#include <mutex>

namespace faabric::util {

#define DEFAULT_LATCH_TIMEOUT_MS 10000

class Latch : public std::enable_shared_from_this<Latch>
{
  public:
    // WARNING: this latch must be shared between threads using a shared
    // pointer, otherwise there seems to be some nasty race conditions related
    // to its destruction.
    static std::shared_ptr<Latch> create(
      int count,
      int timeoutMs = DEFAULT_LATCH_TIMEOUT_MS);

    explicit Latch(int countIn, int timeoutMsIn);

    void wait();

  private:
    int count;
    int waiters = 0;

    int timeoutMs;

    std::mutex mx;
    std::condition_variable cv;
};
}
