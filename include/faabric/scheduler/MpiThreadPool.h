#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <map>
#include <thread>
#include <vector>

#include <faabric/util/exception.h>
#include <faabric/util/queue.h>

namespace faabric::scheduler {
// static faabric::util::Queue<std::pair<int, std::function<void (void)>>>
// jobQueue;
typedef faabric::util::Queue<std::pair<int, std::function<void(void)>>>
  MpiJobQueue;

class MpiAsyncThreadPool
{
  public:
    explicit MpiAsyncThreadPool(int nThreads);

    ~MpiAsyncThreadPool();

    void awaitAsyncRequest(int reqId);

    std::shared_ptr<MpiJobQueue> getMpiJobQueue();

  protected:
    void entrypoint(int i);

  private:
    std::vector<std::thread> threadPool;
    std::mutex awakeMutex;
    std::condition_variable awakeCV;

    // TODO - use instead a data structure that is very easy to lookup. I am
    // thinking of a bit array with zeros and ones in the positions whose binary
    // representation match the reqId. With something like 1024 different reqIds
    // (which we would reuse) it would suffice I think.
    std::map<int, int> asyncReqMap;
    std::shared_ptr<MpiJobQueue> localJobQueue;

    std::atomic<bool> shutdown;
};
}
