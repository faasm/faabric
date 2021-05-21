#include "init.h"
#include "DistTestExecutor.h"

namespace tests {
void initDistTests()
{
    const auto& logger = faabric::util::getLogger();
    logger->info("Registering distributed test server functions");

    tests::registerSchedulerTestFunctions();
}
}
