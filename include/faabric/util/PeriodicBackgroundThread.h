#pragma once

#include <condition_variable>
#include <mutex>
#include <thread>

namespace faabric::util {

/**
 * Wrapper around periodic background thread that repeatedly does some arbitrary
 * work after a given timeout.
 */
class PeriodicBackgroundThread
{
  public:
    void start(int wakeUpPeriodSecondsIn);

    void stop();

    virtual void doWork() = 0;

    virtual void tidyUp();

    int getWakeUpPeriod() {
        return wakeUpPeriodSeconds;
    }

  protected:
    int wakeUpPeriodSeconds;

  private:
    std::unique_ptr<std::jthread> workThread = nullptr;

    std::mutex mx;

    std::condition_variable timeoutCv;
};
}
