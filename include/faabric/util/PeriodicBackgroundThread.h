#pragma once

#include <condition_variable>
#include <mutex>
#include <thread>

#define DEFAULT_BACKGROUND_INTERVAL_SECONDS 30

namespace faabric::util {

/**
 * Wrapper around periodic background thread that repeatedly does some arbitrary
 * work after a given interval.
 */
class PeriodicBackgroundThread
{
  public:
    /**
     * Start the background thread with the given wake-up interval in seconds.
     */
    void start(int intervalSecondsIn);

    /**
     * Stop and wait for this thread to finish.
     */
    void stop();

    virtual void doWork() = 0;

    virtual void tidyUp();

    int getIntervalSeconds() { return intervalSeconds; }

  protected:
    int intervalSeconds = DEFAULT_BACKGROUND_INTERVAL_SECONDS;

  private:
    std::unique_ptr<std::jthread> workThread = nullptr;

    std::mutex mx;

    std::condition_variable_any timeoutCv;
};
}
