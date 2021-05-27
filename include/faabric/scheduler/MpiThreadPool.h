#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <future>
#include <thread>
#include <unordered_set>
#include <vector>

#include <faabric/util/exception.h>
#include <faabric/util/queue.h>

#define QUEUE_SHUTDOWN -1

namespace faabric::scheduler {
typedef std::tuple<int, std::function<void(void)>, std::promise<void>>
  ReqQueueType;
typedef faabric::util::Queue<ReqQueueType> MpiReqQueue;

class MpiAsyncThreadPool
{
  public:
    explicit MpiAsyncThreadPool(int nThreads);

    void shutdown();

    int size;

    std::shared_ptr<MpiReqQueue> getMpiReqQueue();

  private:
    std::vector<std::thread> threadPool;
    std::atomic<bool> isShutdown;

    std::shared_ptr<MpiReqQueue> localReqQueue;

    void entrypoint(int i);
};
}
