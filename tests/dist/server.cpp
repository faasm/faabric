#include "DistTestExecutor.h"
#include "init.h"

#include <faabric/endpoint/Endpoint.h>
#include <faabric/endpoint/FaabricEndpointHandler.h>
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

    int slots = 4;
    SPDLOG_INFO("Forcing distributed test server to have {} slots", slots);
    faabric::HostResources res;
    res.set_slots(slots);
    faabric::scheduler::getScheduler().setThisHostResources(res);

    // WARNING: All 0MQ operations must be contained within their own scope so
    // that all sockets are destructed before the context is closed.
    {
        SPDLOG_INFO("Starting distributed test server on worker");
        std::shared_ptr<ExecutorFactory> fac =
          std::make_shared<tests::DistTestExecutorFactory>();
        faabric::runner::FaabricMain m(fac);
        m.startBackground();

        // Note, endpoint will block until killed
        SPDLOG_INFO("Starting HTTP endpoint on worker");
        const auto& config = faabric::util::getSystemConfig();
        faabric::endpoint::Endpoint endpoint(
          config.endpointPort,
          config.endpointNumThreads,
          std::make_shared<faabric::endpoint::FaabricEndpointHandler>());
        endpoint.start();

        SPDLOG_INFO("Shutting down");
        m.shutdown();
    }

    faabric::transport::closeGlobalMessageContext();

    return EXIT_SUCCESS;
}
