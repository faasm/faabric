#include <faabric/runner/FaabricMain.h>
#include <faabric/scheduler/ExecutorFactory.h>
#include <faabric/scheduler/FunctionCallServer.h>
#include <faabric/util/config.h>
#include <faabric/util/logging.h>

#include <array>
#include <execinfo.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

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

namespace {
const std::string_view ABORT_MSG = "Caught stack backtrace:\n";
constexpr int TEST_SIGNAL = 12341234;

// must be async-signal-safe - don't call allocating functions
void crashHandler(int sig) noexcept
{
    std::array<void*, 32> stackPtrs;
    size_t filledStacks = backtrace(stackPtrs.data(), stackPtrs.size());
    if (sig != TEST_SIGNAL) {
        write(STDERR_FILENO, ABORT_MSG.data(), ABORT_MSG.size());
    }
    backtrace_symbols_fd(stackPtrs.data(),
                         std::min(filledStacks, stackPtrs.size()),
                         STDERR_FILENO);
    if (sig != TEST_SIGNAL) {
        signal(sig, SIG_DFL);
        raise(sig);
        exit(1);
    }
    return;
}
}

void FaabricMain::setupCrashHandler()
{
    fputs("Testing crash handler backtrace:\n", stderr);
    fflush(stderr);
    crashHandler(TEST_SIGNAL);
    SPDLOG_INFO("Installing crash handler");
    for (auto signo : { SIGSEGV, SIGABRT, SIGILL, SIGFPE }) {
        if (signal(signo, &crashHandler) == SIG_ERR) {
            SPDLOG_WARN("Couldn't install handler for signal {}", signo);
        } else {
            SPDLOG_INFO("Installed handler for signal {}", signo);
        }
    }
}

void FaabricMain::startBackground()
{
    // Crash handler
    setupCrashHandler();

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

    SPDLOG_INFO("Waiting for the state server to finish");
    stateServer.stop();

    SPDLOG_INFO("Waiting for the function server to finish");
    functionServer.stop();

    SPDLOG_INFO("Waiting for the snapshot server to finish");
    snapshotServer.stop();

    auto& sch = faabric::scheduler::getScheduler();
    sch.shutdown();

    SPDLOG_INFO("Faabric pool successfully shut down");
}
}
