#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <iostream>
#include <map>
#include <mutex>
#include <thread>
#include <vector>

#include <faabric/util/exception.h>
#include <faabric/util/queue.h>

static faabric::util::Queue<std::pair<int, std::function<void(void)>>> jobQueue;
// TODO - use instead a data structure that is very easy to lookup. I am
// thinking of a bit array with zeros and ones in the positions whose binary
// representation match the reqId. With something like 1024 different reqIds
// (which we would reuse) it would suffice I think.
static std::map<int, int> asyncReqMap;

class MpiAsyncThreadPool
{
  public:
    explicit MpiAsyncThreadPool(int nThreads);

    ~MpiAsyncThreadPool();

    void awaitAsyncRequest(int reqId);

  protected:
    void entrypoint(int i);

  private:
    std::vector<std::thread> threadPool;
    std::mutex awakeMutex;
    std::condition_variable awakeCV;
    std::atomic<bool> shutdown;
};
