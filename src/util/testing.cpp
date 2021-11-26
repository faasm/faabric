#include <faabric/util/testing.h>

#include <atomic>

namespace faabric::util {
static std::atomic_bool testMode = false;
static std::atomic_bool mockMode = false;

void setTestMode(bool val)
{
    testMode.store(val, std::memory_order_release);
}

bool isTestMode()
{
    return testMode.load(std::memory_order_acquire);
}
void setMockMode(bool val)
{
    mockMode.store(val, std::memory_order_release);
}

bool isMockMode()
{
    return mockMode.load(std::memory_order_acquire);
}
}
