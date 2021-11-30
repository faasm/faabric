#pragma once

#include <faabric/util/logging.h>

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <shared_mutex>

#define DEFAULT_FLAG_WAIT_MS 10000

namespace faabric::util {
typedef std::unique_lock<std::mutex> UniqueLock;
typedef std::unique_lock<std::shared_mutex> FullLock;
typedef std::shared_lock<std::shared_mutex> SharedLock;

class FlagWaiter : public std::enable_shared_from_this<FlagWaiter>
{
  public:
    FlagWaiter(int timeoutMsIn = DEFAULT_FLAG_WAIT_MS);

    void waitOnFlag();

    void setFlag(bool value);

  private:
    int timeoutMs;

    std::mutex flagMx;
    std::condition_variable cv;
    std::atomic<bool> flag;
};
}
