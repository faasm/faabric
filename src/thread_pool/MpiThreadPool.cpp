#include <threadPool.h>

MpiAsyncThreadPool::MpiAsyncThreadPool(int nThreads)
    : shutdown(false)
{
    for (int i = 0; i < nThreads; ++i) {
        threadPool.emplace_back(std::bind(&MpiAsyncThreadPool::entrypoint, this, i));
    }
}

MpiAsyncThreadPool::~MpiAsyncThreadPool()
{
    std::cout << "Shutting down MpiAsyncThreadPool" << std::endl;
    this->shutdown = true;

    // When the destructor is called, either some thread will have hit the
    // dequeue timeout and won't be joinable, or none has and all are joinable.
    // We wait untill all hit the timeout, saving an additional cond. variable.
    for (auto& thread : threadPool)
        if (thread.joinable()) {
            thread.join();
        }
}

void MpiAsyncThreadPool::awaitAsyncRequest(int reqId)
{
    faabric::util::UniqueLock lock(awakeMutex);

    // Wait until the condition is signaled and the predicate is false.
    // Note that the same lock protects concurrent accesses to asyncReqMap
    awakeCV.wait(lock, [reqId]{ 
        return asyncReqMap.find(reqId) != asyncReqMap.end();
    });

    // Before giving up the lock, remove our request as it has already finished
    auto it = asyncReqMap.find(reqId);
    if (it == asyncReqMap.end()) {
        std::cout << "Error: something went wrong." << std::endl;
    }
    asyncReqMap.erase(it);
}

void MpiAsyncThreadPool::entrypoint(int i)
{
    std::pair<int, std::function <void (void)>> job;

    while (!this->shutdown)
    {
        // Dequeue blocks until there's something in the queue
        // Note - we assume that if we hit the timeout it's time to shutdown
        // TODO -define the timeout in a constant
        try {
            job = jobQueue.dequeue(5000);
        } catch (const faabric::util::QueueTimeoutException& e) {
            if (!this->shutdown) {
                this->shutdown = true;
            }
            break;
        }

        int reqId = job.first;
        std::function<void (void)> func = job.second;

        std::cout << "Executing on thread: " << i << std::endl;

        // Do the job without holding any locks
        func();

        // Acquire lock to modify the asyncReqMap
        {
            faabric::util::UniqueLock lock(awakeMutex);
            auto it = asyncReqMap.insert(std::make_pair(reqId, 1));
            if (it.second == false) {
                std::cout << "Error: reqId was already in the map!" << std::endl;
            }
        }

        // Notify that the work is done lock-free
        awakeCV.notify_all();
    }

}

/*
int main()
{
    {
        // Create two threads
        MpiAsyncThreadPool p(2);

        // Assign them 4 jobs
        jobQueue.enqueue(std::make_pair(1, std::bind(silly, 1)));
        jobQueue.enqueue(std::make_pair(2, std::bind(silly, 2)));
        jobQueue.enqueue(std::make_pair(3, std::bind(silly, 3)));
        jobQueue.enqueue(std::make_pair(4, std::bind(silly, 4)));

        // Who is doing my job?
        p.awaitAsyncRequest(1);
        p.awaitAsyncRequest(2);
        p.awaitAsyncRequest(3);
        p.awaitAsyncRequest(4);

        std::cout << "we get here" << std::endl;
    }
    return 0;
}
*/
