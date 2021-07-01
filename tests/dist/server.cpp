#include "DistTestExecutor.h"
#include "init.h"

#include <faabric/endpoint/FaabricEndpoint.h>
#include <faabric/runner/FaabricMain.h>
#include <faabric/scheduler/ExecutorFactory.h>
#include <faabric/transport/context.h>
#include <faabric/util/logging.h>

using namespace faabric::scheduler;

int main()
{
    faabric::util::initLogging();
    faabric::transport::initGlobalMessageContext();
    tests::initDistTests();

    SPDLOG_INFO("Starting distributed test server on worker");
    std::shared_ptr<ExecutorFactory> fac =
      std::make_shared<tests::DistTestExecutorFactory>();
    faabric::runner::FaabricMain m(fac);
    m.startBackground();

    // Note, endpoint will block until killed
    SPDLOG_INFO("Starting HTTP endpoint on worker");
    faabric::endpoint::FaabricEndpoint endpoint;
    endpoint.start();

    SPDLOG_INFO("Shutting down");
    m.shutdown();
    faabric::transport::closeGlobalMessageContext();

    return EXIT_SUCCESS;
}
