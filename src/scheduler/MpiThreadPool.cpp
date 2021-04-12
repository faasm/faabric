#include <faabric/scheduler/MpiThreadPool.h>
#include <faabric/util/logging.h>

namespace faabric::scheduler {
MpiAsyncThreadPool::MpiAsyncThreadPool(int nThreads)
  : shutdown(false)
{
    faabric::util::getLogger()->debug(
      "Starting an MpiAsyncThreadPool of size {}", nThreads);

    // Initialize async. req queue
    localReqQueue = std::make_shared<MpiReqQueue>();

    // Initialize thread pool
    for (int i = 0; i < nThreads; ++i) {
        threadPool.emplace_back(
          std::bind(&MpiAsyncThreadPool::entrypoint, this, i));
    }
}

MpiAsyncThreadPool::~MpiAsyncThreadPool()
{
    faabric::util::getLogger()->debug("Shutting down MpiAsyncThreadPool");
    this->shutdown = true;

    // Load the queue with shutdown messages, i.e. nullptr functions
    std::function<void(void)> f;
    for (int i = 0; i < this->threadPool.size(); i++) {
        std::promise<void> p;
        this->getMpiReqQueue()->enqueue(
          std::make_pair(-1, std::make_pair(f, std::move(p))));
    }

    for (auto& thread : threadPool) {
        if (thread.joinable()) {
            thread.join();
        }
    }
}

std::shared_ptr<MpiReqQueue> MpiAsyncThreadPool::getMpiReqQueue()
{
    return this->localReqQueue;
}

void MpiAsyncThreadPool::entrypoint(int i)
{
    faabric::scheduler::ReqQueueType req;

    while (!this->shutdown) {
        req = getMpiReqQueue()->dequeue();

        std::function<void(void)> func = req.second.first;
        std::promise<void> promise = std::move(req.second.second);

        // Detect shutdown condition
        if (!func) {
            if (!this->shutdown) {
                this->shutdown = true;
            }
            break;
        }

        // Do the job without holding any locks
        func();

        // Notify we are done via the future
        promise.set_value();
    }
}
}
