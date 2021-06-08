#include "init.h"
#include "DistTestExecutor.h"

namespace tests {
void initDistTests()
{

    SPDLOG_INFO("Registering distributed test server functions");

    tests::registerSchedulerTestFunctions();
}
}
