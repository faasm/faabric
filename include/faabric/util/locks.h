#pragma once

#include <faabric/util/logging.h>

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <shared_mutex>

namespace faabric::util {
typedef std::unique_lock<std::mutex> UniqueLock;
typedef std::unique_lock<std::shared_mutex> FullLock;
typedef std::shared_lock<std::shared_mutex> SharedLock;

class FlagWaiter
{
  public:
    void waitOnFlag()
    {
        // Check
        if (flag.load()) {
            return;
        }

        // Wait for group to be enabled
        UniqueLock lock(flagMx);
        if (!cv.wait_for(lock, std::chrono::milliseconds(10000), [this] {
                return flag.load();
            })) {

            SPDLOG_ERROR("Timed out waiting for flag");
            throw std::runtime_error("Timed out waiting for flag");
        }
    }

    void setFlag(bool value) { flag.store(value); }

    bool getValue() { return flag.load(); }

  private:
    std::mutex flagMx;
    std::condition_variable cv;
    std::atomic<bool> flag;
};
}
