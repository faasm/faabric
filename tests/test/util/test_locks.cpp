#include <catch2/catch.hpp>
#include <faabric/util/latch.h>

#include <faabric/util/latch.h>
#include <faabric/util/locks.h>
#include <faabric/util/macros.h>

using namespace faabric::util;

namespace tests {

TEST_CASE("Test wait flag", "[util]")
{
    int nThreads = 10;

    auto flagA = std::make_shared<FlagWaiter>();
    auto flagB = std::make_shared<FlagWaiter>();

    std::shared_ptr<Latch> latchA1 = Latch::create(nThreads + 1);
    std::shared_ptr<Latch> latchB1 = Latch::create(nThreads + 1);

    std::shared_ptr<Latch> latchA2 = Latch::create(nThreads + 1);
    std::shared_ptr<Latch> latchB2 = Latch::create(nThreads + 1);

    std::vector<int> expectedUnset(nThreads, 0);
    std::vector<int> expectedSet;

    std::vector<std::thread> threadsA;
    std::vector<std::thread> threadsB;

    std::vector<int> resultsA(nThreads, 0);
    std::vector<int> resultsB(nThreads, 0);

    for (int i = 0; i < nThreads; i++) {
        expectedSet.push_back(i);

        threadsA.emplace_back([&flagA, &resultsA, &latchA1, &latchA2, i] {
            latchA1->wait();
            flagA->waitOnFlag();
            resultsA[i] = i;
            latchA2->wait();
        });

        threadsB.emplace_back([&flagB, &resultsB, &latchB1, &latchB2, i] {
            latchB1->wait();
            flagB->waitOnFlag();
            resultsB[i] = i;
            latchB2->wait();
        });
    }

    // Check no results are set initially
    latchA1->wait();
    latchB1->wait();
    REQUIRE(resultsA == expectedUnset);
    REQUIRE(resultsB == expectedUnset);

    // Set one flag, await latch and check
    flagA->setFlag(true);
    latchA2->wait();
    REQUIRE(resultsA == expectedSet);
    REQUIRE(resultsB == expectedUnset);

    // Set other flag, await latch and check
    flagB->setFlag(true);
    latchB2->wait();
    REQUIRE(resultsA == expectedSet);
    REQUIRE(resultsB == expectedSet);

    for (auto& t : threadsA) {
        if (t.joinable()) {
            t.join();
        }
    }

    for (auto& t : threadsB) {
        if (t.joinable()) {
            t.join();
        }
    }
}
}
