#pragma once

#include <faabric/util/logging.h>

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <shared_mutex>

#define DEFAULT_FLAG_WAIT_MS 10000

namespace faabric::util {
typedef std::unique_lock<std::mutex> UniqueLock;
typedef std::unique_lock<std::shared_mutex> FullLock;
typedef std::shared_lock<std::shared_mutex> SharedLock;

class FlagWaiter
{
  public:
    FlagWaiter(int timeoutMsIn = DEFAULT_FLAG_WAIT_MS)
      : timeoutMs(timeoutMsIn)
    {}

    void waitOnFlag()
    {
        // Check
        if (flag.load()) {
            return;
        }

        // Wait for group to be enabled
        UniqueLock lock(flagMx);
        if (!cv.wait_for(lock, std::chrono::milliseconds(timeoutMs), [this] {
                return flag.load();
            })) {

            SPDLOG_ERROR("Timed out waiting for flag");
            throw std::runtime_error("Timed out waiting for flag");
        }
    }

    void setFlag(bool value)
    {
        UniqueLock lock(flagMx);
        flag.store(value);
        cv.notify_all();
    }

  private:
    int timeoutMs;

    std::mutex flagMx;
    std::condition_variable cv;
    std::atomic<bool> flag;
};
}
