#include <faabric/mpi-native/MpiExecutor.h>
#include <faabric/transport/context.h>
#include <faabric/util/logging.h>

namespace faabric::mpi_native {

faabric::Message* executingCall;
int mpiFunc();

MpiExecutor::MpiExecutor(faabric::Message& msg)
  : Executor(msg){};

int32_t MpiExecutor::executeTask(
  int threadPoolIdx,
  int msgIdx,
  std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    faabric::mpi_native::executingCall = &req->mutable_messages()->at(msgIdx);

    int error = mpiFunc();
    if (error) {
        SPDLOG_ERROR("There was an error running the MPI function");
    }

    return 0;
}

int mpiNativeMain(int argc, char** argv)
{
    faabric::transport::initGlobalMessageContext();

    auto& scheduler = faabric::scheduler::getScheduler();
    auto& conf = faabric::util::getSystemConfig();

    bool __isRoot;
    int __worldSize;
    if (argc < 2) {
        SPDLOG_DEBUG("Non-root process started");
        __isRoot = false;
    } else if (argc < 3) {
        SPDLOG_ERROR("Root process started without specifying world size!");
        return 1;
    } else {
        SPDLOG_DEBUG("Root process started");
        __worldSize = std::stoi(argv[2]);
        __isRoot = true;
        SPDLOG_DEBUG("MPI World Size: {}", __worldSize);
    }

    // Force this host to run one thread
    conf.overrideCpuCount = 1;

    // Pre-load message to bootstrap execution
    if (__isRoot) {
        faabric::Message msg = faabric::util::messageFactory("mpi", "exec");
        msg.set_mpiworldsize(__worldSize);
        scheduler.callFunction(msg);
    }

    faabric::transport::closeGlobalMessageContext();

    return 0;
}
}
