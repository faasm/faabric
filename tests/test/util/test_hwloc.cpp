#include <catch2/catch.hpp>

#include <faabric/util/hwloc.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>
#include <faabric/util/testing.h>

#include <pthread.h>
#include <thread>

using namespace faabric::util;

namespace tests {
int nCpus = std::jthread::hardware_concurrency();

void checkThreadIsPinnedToCpu(pthread_t thread, int cpuIdx)
{
    // Check the affinity given by pthread
    cpu_set_t actualCpuSet;
    int retVal =
      pthread_getaffinity_np(thread, sizeof(cpu_set_t), &actualCpuSet);
    REQUIRE(retVal == 0);

    REQUIRE(CPU_ISSET(cpuIdx, &actualCpuSet));
}

TEST_CASE("Test pinning thread to CPU using pthreads")
{
    pthread_t self = pthread_self();
    REQUIRE(nCpus > 0);

    auto cpuSet = pinThreadToFreeCpu(self);

    REQUIRE(cpuSet->get() != nullptr);

    checkThreadIsPinnedToCpu(self, 0);
}

TEST_CASE("Test pinning two threads to the same CPU")
{
    pthread_t self = pthread_self();
    std::jthread other([] { SLEEP_MS(200); });

    auto cpuSet = pinThreadToFreeCpu(self);
    pinThreadToCpu(other.native_handle(), cpuSet->get());

    checkThreadIsPinnedToCpu(self, 0);
    checkThreadIsPinnedToCpu(other.native_handle(), 0);
}

TEST_CASE("Test pinning many pairs of threads")
{
    std::vector<std::jthread> someThreads;
    std::vector<std::jthread> otherThreads;
    std::vector<std::unique_ptr<FaabricCpuSet>> someCpuSets;

    // First, occupy all CPUs
    for (int i = 0; i < nCpus; i++) {
        someThreads.emplace_back([] { SLEEP_MS(200); });
        otherThreads.emplace_back([] { SLEEP_MS(200); });
        someCpuSets.emplace_back(
          pinThreadToFreeCpu(someThreads.at(i).native_handle()));
        pinThreadToCpu(otherThreads.at(i).native_handle(),
                       someCpuSets.at(i)->get());

        checkThreadIsPinnedToCpu(someThreads.at(i).native_handle(), i);
        checkThreadIsPinnedToCpu(otherThreads.at(i).native_handle(), i);
    }
}

TEST_CASE("Test pinning (null-pointing) thread to CPU")
{
    REQUIRE_THROWS(pinThreadToCpu(pthread_self(), nullptr));
}

TEST_CASE("Test overcommitting to CPU cores fails unless in test mode")
{
    std::vector<std::jthread> threads;
    std::vector<std::unique_ptr<FaabricCpuSet>> cpuSets;

    // First, occupy all CPUs
    for (int i = 0; i < nCpus; i++) {
        threads.emplace_back([] { SLEEP_MS(200); });
        cpuSets.emplace_back(pinThreadToFreeCpu(threads.at(i).native_handle()));
        checkThreadIsPinnedToCpu(threads.at(i).native_handle(), i);
    }

    bool isTestMode;
    SECTION("Test mode disabled")
    {
        isTestMode = false;
    }

    SECTION("Test mode enabled")
    {
        isTestMode = true;
    }

    // Then, try to occupy another one
    std::unique_ptr<FaabricCpuSet> lastCpuSet;
    pthread_t self = pthread_self();
    faabric::util::setTestMode(isTestMode);

    if (isTestMode) {
        // In test mode, this is allowed
        REQUIRE_NOTHROW(lastCpuSet = pinThreadToFreeCpu(self));
        checkThreadIsPinnedToCpu(self, 0);
    } else {
        // In non-test mode, it is not
        REQUIRE_THROWS(lastCpuSet = pinThreadToFreeCpu(self));
    }

    faabric::util::setTestMode(true);
}
}
