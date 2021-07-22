#include <catch.hpp>

#include "faabric_utils.h"

#include <faabric/util/barrier.h>
#include <faabric/util/bytes.h>
#include <faabric/util/macros.h>

#include <thread>
#include <unistd.h>

using namespace faabric::util;

namespace tests {
TEST_CASE("Test barrier operation", "[util]")
{
    int nThreads = 5;
    int nSums = 1000;
    std::vector<std::atomic<int>> sums(nSums);

    std::atomic<int> completionCount = 0;

    // Shared barrier between all threads
    auto b =
      Barrier::create(nThreads, [&completionCount] { completionCount++; });

    // Have n-1 threads iterating through sums, adding, then waiting on the
    // barrier
    std::vector<std::thread> threads;
    for (int i = 1; i < nThreads; i++) {
        threads.emplace_back([nSums, &b, &sums]() {
            for (int s = 0; s < nSums; s++) {
                sums.at(s).fetch_add(s + 1);
                b->wait();
            }
        });
    }

    for (int s = 0; s < nSums; s++) {
        b->wait();

        REQUIRE(sums.at(s) == (nThreads - 1) * (s + 1));

        // Only the latest completion function should have run
        REQUIRE(completionCount == s + 1);
    }

    for (auto& t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }
}
}
