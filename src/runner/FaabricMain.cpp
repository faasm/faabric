#include <faabric/runner/FaabricMain.h>
#include <faabric/scheduler/ExecutorFactory.h>
#include <faabric/scheduler/FunctionCallServer.h>
#include <faabric/util/config.h>
#include <faabric/util/logging.h>

#if (FAASM_SGX)
namespace sgx {
extern void checkSgxSetup();
}
#endif

namespace faabric::runner {
FaabricMain::FaabricMain(
  std::shared_ptr<faabric::scheduler::ExecutorFactory> execFactory)
  : stateServer(faabric::state::getGlobalState())
{
    faabric::scheduler::setExecutorFactory(execFactory);
}

void FaabricMain::startBackground()
{
    // Ensure we can ping both redis instances
    faabric::redis::Redis::getQueue().ping();
    faabric::redis::Redis::getState().ping();

    auto& sch = faabric::scheduler::getScheduler();
    sch.addHostToGlobalSet();

    faabric::util::SystemConfig& conf = faabric::util::getSystemConfig();
    conf.print();

#if (FAASM_SGX)
    // Check for SGX capability and create shared enclave
    sgx::checkSgxSetup();
#endif

    // In-memory state
    startStateServer();

    // Snapshots
    startSnapshotServer();

    // Work sharing
    startFunctionCallServer();
}

void FaabricMain::startFunctionCallServer()
{
    const std::shared_ptr<spdlog::logger>& logger = faabric::util::getLogger();
    logger->info("Starting function call server");
    functionServer.start();
}

void FaabricMain::startSnapshotServer()
{
    auto logger = faabric::util::getLogger();
    logger->info("Starting snapshot server");
    snapshotServer.start();
}

void FaabricMain::startStateServer()
{
    const std::shared_ptr<spdlog::logger>& logger = faabric::util::getLogger();

    // Skip state server if not in inmemory mode
    faabric::util::SystemConfig& conf = faabric::util::getSystemConfig();
    if (conf.stateMode != "inmemory") {
        logger->info("Not starting state server in state mode {}",
                     conf.stateMode);
        return;
    }

    // Note that the state server spawns its own background thread
    logger->info("Starting state server");
    stateServer.start();
}

void FaabricMain::shutdown()
{
    const auto& logger = faabric::util::getLogger();
    logger->info("Removing from global working set");

    auto& sch = faabric::scheduler::getScheduler();
    sch.shutdown();

    logger->info("Waiting for the state server to finish");
    stateServer.stop();

    logger->info("Waiting for the function server to finish");
    functionServer.stop();

    logger->info("Waiting for the snapshot server to finish");
    snapshotServer.stop();

    logger->info("Faabric pool successfully shut down");
}
}
