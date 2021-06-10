#include "init.h"
#include "DistTestExecutor.h"

#include <faabric/util/logging.h>

namespace tests {
void initDistTests()
{
    SPDLOG_INFO("Registering distributed test server functions");

    tests::registerSchedulerTestFunctions();
}
}
