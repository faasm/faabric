#include <catch.hpp>

#include <faabric/util/gids.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>
#include <set>
#include <thread>

using namespace faabric::util;

namespace tests {
TEST_CASE("Test multithreaded gid generation", "[util]")
{
    // Generate gids
    int nThreads = 10;
    int nLoops = 1000;
    int nValues = nThreads * nLoops;

    std::vector<unsigned int> generated;
    std::mutex mx;
    std::vector<std::thread> threads(nThreads);
    for (int i = 0; i < nThreads; i++) {
        threads.emplace_back(std::thread([&generated, &mx, nLoops] {
            for (int j = 0; j < nLoops; j++) {
                faabric::util::UniqueLock lock(mx);
                generated.push_back(faabric::util::generateGid());
            }
        }));
    }

    for (auto& t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }

    REQUIRE(generated.size() == nValues);

    // Check that there are no duplicates (if there's a problem there should
    // reliably be several)
    std::set<unsigned int> uniques;
    for (auto g : generated) {
        if (uniques.count(g) > 0) {
            const std::shared_ptr<spdlog::logger>& logger =
              faabric::util::getLogger();
            logger->error("Found duplicate gid - {}", g);
            FAIL();
        } else {
            uniques.insert(g);
        }
    }
}
}
