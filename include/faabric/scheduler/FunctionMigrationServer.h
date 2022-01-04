#pragma once

#include <condition_variable>
#include <mutex>
#include <thread>

namespace faabric::scheduler {
class FunctionMigrationServer
{
  public:
    void start();

    void stop();

  private:
    std::unique_ptr<std::thread> workThread = nullptr;
    std::mutex mx;
    std::condition_variable mustStopCv;
    std::atomic<bool> isShutdown;
};
}
