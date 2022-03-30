#include <catch2/catch.hpp>

#include "faabric_utils.h"

#include <faabric/util/bytes.h>
#include <faabric/util/latch.h>
#include <faabric/util/macros.h>

#include <thread>
#include <unistd.h>

using namespace faabric::util;

namespace tests {
TEST_CASE("Test latch operation", "[util]")
{
    auto l = Latch::create(3);

    auto t1 = std::jthread([l] { l->wait(); });
    auto t2 = std::jthread([l] { l->wait(); });

    l->wait();

    if (t1.joinable()) {
        t1.join();
    }

    if (t2.joinable()) {
        t2.join();
    }

    REQUIRE_THROWS(l->wait());
}

TEST_CASE("Test latch timeout", "[util]")
{
    int timeoutMs = 500;
    auto l = Latch::create(2, timeoutMs);
    REQUIRE_THROWS(l->wait());
}
}
