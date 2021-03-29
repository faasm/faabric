#include <faabric/util/testing.h>

namespace faabric::util {
static bool testMode = false;
static bool mockMode = false;

void setTestMode(bool val)
{
    testMode = val;
}

bool isTestMode()
{
    return testMode;
}
void setMockMode(bool val)
{
    mockMode = val;
}

bool isMockMode()
{
    return mockMode;
}
}
