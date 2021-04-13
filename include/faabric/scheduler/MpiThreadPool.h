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

namespace faabric::scheduler {
typedef std::tuple<int, std::function<void(void)>, std::promise<void>>
  ReqQueueType;
typedef faabric::util::Queue<ReqQueueType> MpiReqQueue;

class MpiAsyncThreadPool
{
  public:
    explicit MpiAsyncThreadPool(int nThreads);

    ~MpiAsyncThreadPool();

    std::shared_ptr<MpiReqQueue> getMpiReqQueue();

  private:
    std::vector<std::thread> threadPool;
    std::atomic<bool> shutdown;

    std::shared_ptr<MpiReqQueue> localReqQueue;

    void entrypoint(int i);
};
}
