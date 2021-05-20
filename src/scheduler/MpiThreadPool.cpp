#include <faabric/scheduler/MpiThreadPool.h>
#include <faabric/util/logging.h>

namespace faabric::scheduler {
MpiAsyncThreadPool::MpiAsyncThreadPool(int nThreads)
  : size(nThreads)
  , shutdown(false)
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

        int id = std::get<0>(req);
        std::function<void(void)> func = std::get<1>(req);
        std::promise<void> promise = std::move(std::get<2>(req));

        // Detect shutdown condition
        if (id == QUEUE_SHUTDOWN) {
            // The shutdown tuple includes a TLS cleanup function that we run
            // _once per thread_ and exit
            func();
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
