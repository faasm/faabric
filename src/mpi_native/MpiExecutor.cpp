#include <faabric/mpi-native/MpiExecutor.h>

namespace faabric::mpi_native {

faabric::Message* executingCall;
int mpiFunc();

MpiExecutor::MpiExecutor(const faabric::Message& msg)
  : Executor(msg){};

int32_t MpiExecutor::executeTask(
  int threadPoolIdx,
  int msgIdx,
  std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    auto logger = faabric::util::getLogger();

    faabric::mpi_native::executingCall = &req->mutable_messages()->at(msgIdx);

    int error = mpiFunc();
    if (error) {
        logger->error("There was an error running the MPI function");
    }

    return 0;
}

int mpiNativeMain(int argc, char** argv)
{
    auto logger = faabric::util::getLogger();
    auto& scheduler = faabric::scheduler::getScheduler();
    auto& conf = faabric::util::getSystemConfig();

    bool __isRoot;
    int __worldSize;
    if (argc < 2) {
        logger->debug("Non-root process started");
        __isRoot = false;
    } else if (argc < 3) {
        logger->error("Root process started without specifying world size!");
        return 1;
    } else {
        logger->debug("Root process started");
        __worldSize = std::stoi(argv[2]);
        __isRoot = true;
        logger->debug("MPI World Size: {}", __worldSize);
    }

    // Force this host to run one thread
    conf.overrideCpuCount = 1;

    // Pre-load message to bootstrap execution
    if (__isRoot) {
        faabric::Message msg = faabric::util::messageFactory("mpi", "exec");
        msg.set_mpiworldsize(__worldSize);
        scheduler.callFunction(msg);
    }

    return 0;
}
}
