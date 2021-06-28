#include <catch.hpp>

#include "faabric_utils.h"

#include <faabric/util/bytes.h>
#include <faabric/util/macros.h>
#include <faabric/util/queue.h>

#include <future>
#include <thread>
#include <unistd.h>

using namespace faabric::util;

typedef faabric::util::Queue<int> IntQueue;

namespace tests {
TEST_CASE("Test queue operations", "[util]")
{
    IntQueue q;

    // Check deqeue if present does nothing if nothing in queue
    int dummy = -999;
    q.dequeueIfPresent(&dummy);
    REQUIRE(dummy == -999);

    // Enqueue a bunch of elements
    q.enqueue(1);
    q.enqueue(2);
    q.enqueue(3);
    q.enqueue(4);
    q.enqueue(5);

    // Dequeue
    REQUIRE(q.dequeue() == 1);
    REQUIRE(q.dequeue() == 2);

    // Check peek doesn't remove
    REQUIRE(*(q.peek()) == 3);
    REQUIRE(*(q.peek()) == 3);
    REQUIRE(*(q.peek()) == 3);
    REQUIRE(q.dequeue() == 3);

    // Check dequeue if present removes when something is there
    q.dequeueIfPresent(&dummy);
    REQUIRE(dummy == 4);

    // Check a final dequeue
    REQUIRE(q.dequeue() == 5);

    // Check error thrown on timeout when waiting
    REQUIRE_THROWS(q.dequeue(1));
}

TEST_CASE("Test drain queue", "[util]")
{
    IntQueue q;

    q.enqueue(1);
    q.enqueue(2);
    q.enqueue(3);

    REQUIRE(q.size() == 3);

    q.drain();

    REQUIRE(q.size() == 0);
}

TEST_CASE("Test wait for draining empty queue", "[util]")
{
    // Just need to check this doesn't fail
    IntQueue q;
    q.waitToDrain(100);
}

TEST_CASE("Test wait for draining queue with elements", "[util]")
{
    IntQueue q;
    int nElems = 5;
    std::vector<int> dequeued;
    std::vector<int> expected;

    for (int i = 0; i < nElems; i++) {
        q.enqueue(i);
        expected.emplace_back(i);
    }

    // Background thread to consume elements
    std::thread t([&q, &dequeued, nElems] {
        for (int i = 0; i < nElems; i++) {
            SLEEP_MS(100);

            int j = q.dequeue();
            dequeued.emplace_back(j);
        }
    });

    q.waitToDrain(2000);

    if (t.joinable()) {
        t.join();
    }

    REQUIRE(dequeued == expected);
}

TEST_CASE("Test queue on non-copy-constructible object", "[util]")
{
    faabric::util::Queue<std::promise<int32_t>> q;

    std::promise<int32_t> a;
    std::promise<int32_t> b;

    std::future<int32_t> fa = a.get_future();
    std::future<int32_t> fb = b.get_future();

    q.enqueue(std::move(a));
    q.enqueue(std::move(b));

    std::thread ta([&q] { q.dequeue().set_value(1); });
    std::thread tb([&q] {
        SLEEP_MS(SHORT_TEST_TIMEOUT_MS);
        q.dequeue().set_value(2);
    });

    if (ta.joinable()) {
        ta.join();
    }
    if (tb.joinable()) {
        tb.join();
    }

    REQUIRE(fa.get() == 1);
    REQUIRE(fb.get() == 2);
}

TEST_CASE("Test queue timeout must be positive", "[util]")
{
    int timeoutValueMs;

    SECTION("Zero timeout") { timeoutValueMs = 0; }

    SECTION("Negative timeout") { timeoutValueMs = -1; }

    faabric::util::Queue<int> q;
    q.enqueue(10);
    REQUIRE_THROWS(q.dequeue(timeoutValueMs));
}
}
