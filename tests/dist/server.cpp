#include "init.h"
#include "DistTestExecutor.h"

#include <faabric/endpoint/FaabricEndpoint.h>
#include <faabric/runner/FaabricMain.h>
#include <faabric/scheduler/ExecutorFactory.h>
#include <faabric/util/logging.h>

using namespace faabric::scheduler;

int main()
{
    const auto& logger = faabric::util::getLogger();

    tests::initDistTests();

    logger->info("Starting executor pool in the background");
    std::shared_ptr<ExecutorFactory> fac =
      std::make_shared<tests::DistTestExecutorFactory>();
    faabric::runner::FaabricMain m(fac);
    m.startBackground();

    logger->info("Starting distributed test server");
    faabric::endpoint::FaabricEndpoint endpoint;
    endpoint.start();

    logger->info("Shutting down");
    m.shutdown();

    return EXIT_SUCCESS;
}
