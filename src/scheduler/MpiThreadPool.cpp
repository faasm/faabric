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

    // When the destructor is called, either some thread will have hit the
    // dequeue timeout and won't be joinable, or none has and all are joinable.
    // We wait untill all hit the timeout, saving an additional cond. variable.
    for (auto& thread : threadPool) {
        if (thread.joinable()) {
            thread.join();
        }
    }
}

void MpiAsyncThreadPool::awaitAsyncRequest(int reqId)
{
    faabric::util::UniqueLock lock(awakeMutex);

    // Wait until the condition is signaled and the predicate is false.
    // Note that the same lock protects concurrent accesses to finishedReqs
    awakeCV.wait(lock, [this, reqId] {
        return this->finishedReqs.find(reqId) != this->finishedReqs.end();
    });

    // Before giving up the lock, remove our request as it has already finished
    auto it = finishedReqs.find(reqId);
    if (it == finishedReqs.end()) {
        throw std::runtime_error(
          fmt::format("Error: unrecognized reqId {}", reqId));
    }
    finishedReqs.erase(it);
}

std::shared_ptr<MpiReqQueue> MpiAsyncThreadPool::getMpiReqQueue()
{
    return this->localReqQueue;
}

void MpiAsyncThreadPool::entrypoint(int i)
{
    std::pair<int, std::function<void(void)>> req;

    while (!this->shutdown) {
        // Dequeue blocks until there's something in the queue
        // Note - we assume that if we hit the timeout it's time to shutdown
        // TODO -define the timeout in a constant
        try {
            req = getMpiReqQueue()->dequeue(5000);
        } catch (const faabric::util::QueueTimeoutException& e) {
            if (!this->shutdown) {
                this->shutdown = true;
            }
            break;
        }

        int reqId = req.first;
        std::function<void(void)> func = req.second;

        // Do the job without holding any locks
        func();

        // Acquire lock to modify the finishedReqs set
        {
            faabric::util::UniqueLock lock(awakeMutex);
            auto it = finishedReqs.insert(reqId);
            if (it.second == false) {
                throw std::runtime_error(
                  fmt::format("Error: reqId collision {}", reqId));
            }
        }

        // Notify that the work is done lock-free
        awakeCV.notify_all();
    }
}
}
