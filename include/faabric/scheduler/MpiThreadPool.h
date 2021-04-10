#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <unordered_set>
#include <thread>
#include <vector>

#include <faabric/util/exception.h>
#include <faabric/util/queue.h>

namespace faabric::scheduler {
typedef faabric::util::Queue<std::pair<int, std::function<void(void)>>>
  MpiReqQueue;

class MpiAsyncThreadPool
{
  public:
    explicit MpiAsyncThreadPool(int nThreads);

    ~MpiAsyncThreadPool();

    void awaitAsyncRequest(int reqId);

    std::shared_ptr<MpiReqQueue> getMpiReqQueue();

  private:
    std::vector<std::thread> threadPool;
    std::mutex awakeMutex;
    std::condition_variable awakeCV;
    std::atomic<bool> shutdown;

    // TODO - use instead a data structure that is very easy to lookup. I am
    // thinking of a bit array with zeros and ones in the positions whose binary
    // representation match the reqId. With something like 1024 different reqIds
    // (which we would reuse) it would suffice I think.
    std::unordered_set<int> finishedReqs;
    std::shared_ptr<MpiReqQueue> localReqQueue;

    void entrypoint(int i);
};
}
