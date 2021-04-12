#include <faabric/scheduler/MpiThreadPool.h>

#include <future>
#include <iostream>

void silly(int n)
{
    // A silly job for demonstration purposes
    std::cout << "Sleeping for " << n << " seconds" << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(n));
}

int main()
{
    {
        std::cout << "Starting" << std::endl;
        // Create two threads
        faabric::scheduler::MpiAsyncThreadPool p(2);
        auto jobQueue = p.getMpiReqQueue();

        // Assign them 4 jobs
        std::unordered_map<int, std::future<void>> futureMap;
        for (int i = 1; i <= 4; i++) {
            std::promise<void> promise;
            std::future<void> future = promise.get_future();
            jobQueue->enqueue(std::make_pair(
              i, std::make_pair(std::bind(silly, i), std::move(promise))));
            futureMap.emplace(std::make_pair(i, std::move(future)));
        }
        // jobQueue->enqueue(std::make_pair(2, std::bind(silly, 2)));
        // jobQueue->enqueue(std::make_pair(3, std::bind(silly, 3)));
        // jobQueue->enqueue(std::make_pair(4, std::bind(silly, 4)));

        // Who is doing my job?
        for (int i = 1; i <= 4; i++) {
            auto it = futureMap.find(i);
            it->second.wait();
            futureMap.erase(it);
        }
        // p.awaitAsyncRequest(2);
        // p.awaitAsyncRequest(3);
        // p.awaitAsyncRequest(4);

        std::cout << "we get here" << std::endl;
    }
    return 0;
}
