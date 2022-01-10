#pragma once

#include <condition_variable>
#include <mutex>
#include <thread>

namespace faabric::scheduler {
// Start a background thread that, every wake up period, will check if there
// are migration opportunities for in-flight apps that have opted in to
// being checked for migrations.
class FunctionMigrationThread
{
  public:
    void start(int wakeUpPeriodSecondsIn);

    void stop();

    int wakeUpPeriodSeconds;

  private:
    std::unique_ptr<std::thread> workThread = nullptr;
    std::mutex mx;
    std::condition_variable mustStopCv;
    std::atomic<bool> isShutdown;
};
}
