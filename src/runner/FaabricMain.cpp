#include <faabric/runner/FaabricMain.h>
#include <faabric/scheduler/ExecutorFactory.h>
#include <faabric/scheduler/FunctionCallServer.h>
#include <faabric/transport/context.h>
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
    // Start basics
    startRunner();

    // In-memory state
    startStateServer();

    // Snapshots
    startSnapshotServer();

    // Work sharing
    startFunctionCallServer();
}

void FaabricMain::startRunner()
{
    // Ensure we can ping both redis instances
    faabric::redis::Redis::getQueue().ping();
    faabric::redis::Redis::getState().ping();

    auto& sch = faabric::scheduler::getScheduler();
    sch.addHostToGlobalSet();

#if (FAASM_SGX)
    // Check for SGX capability and create shared enclave
    sgx::checkSgxSetup();
#endif
}

void FaabricMain::startFunctionCallServer()
{
    SPDLOG_INFO("Starting function call server");
    functionServer.start();
}

void FaabricMain::startSnapshotServer()
{
    SPDLOG_INFO("Starting snapshot server");
    snapshotServer.start();
}

void FaabricMain::startStateServer()
{
    // Skip state server if not in inmemory mode
    faabric::util::SystemConfig& conf = faabric::util::getSystemConfig();
    if (conf.stateMode != "inmemory") {
        SPDLOG_INFO("Not starting state server in state mode {}",
                    conf.stateMode);
        return;
    }

    // Note that the state server spawns its own background thread
    SPDLOG_INFO("Starting state server");
    stateServer.start();
}

void FaabricMain::shutdown()
{
    SPDLOG_INFO("Removing from global working set");

    auto& sch = faabric::scheduler::getScheduler();
    sch.shutdown();

    SPDLOG_INFO("Waiting for the state server to finish");
    stateServer.stop();

    SPDLOG_INFO("Waiting for the function server to finish");
    functionServer.stop();

    SPDLOG_INFO("Waiting for the snapshot server to finish");
    snapshotServer.stop();

    SPDLOG_INFO("Faabric pool successfully shut down");
}
}
