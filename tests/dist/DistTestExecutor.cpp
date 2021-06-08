#include "DistTestExecutor.h"

#include <sys/mman.h>

#include <faabric/snapshot/SnapshotRegistry.h>

using namespace faabric::scheduler;

namespace tests {

static std::unordered_map<std::string, ExecutorFunction> executorFunctions;

void registerDistTestExecutorCallback(const char* user,
                                      const char* funcName,
                                      ExecutorFunction func)
{
    std::string key = std::string(user) + "_" + std::string(funcName);
    executorFunctions[key] = func;


    SPDLOG_DEBUG("Registered executor callback for {}", key);
}

ExecutorFunction getDistTestExecutorCallback(const faabric::Message& msg)
{
    std::string key = msg.user() + "_" + msg.function();
    if (executorFunctions.find(key) == executorFunctions.end()) {

        SPDLOG_ERROR("No registered executor callback for {}", key);
        throw std::runtime_error(
          "Could not find executor callback for function");
    }

    return executorFunctions[key];
}

DistTestExecutor::DistTestExecutor(const faabric::Message& msg)
  : Executor(msg)
{}

DistTestExecutor::~DistTestExecutor() {}

int32_t DistTestExecutor::executeTask(
  int threadPoolIdx,
  int msgIdx,
  std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    const faabric::Message& msg = req->mutable_messages()->at(msgIdx);

    // Look up function and invoke
    ExecutorFunction callback = getDistTestExecutorCallback(msg);
    return callback(this, threadPoolIdx, msgIdx, req);
}

faabric::util::SnapshotData DistTestExecutor::snapshot()
{
    faabric::util::SnapshotData snap;
    snap.data = snapshotMemory;
    snap.size = snapshotSize;

    return snap;
}

void DistTestExecutor::restore(const faabric::Message& msg)
{
    // Initialise the dummy memory and map to snapshot
    faabric::snapshot::SnapshotRegistry& reg =
      faabric::snapshot::getSnapshotRegistry();
    faabric::util::SnapshotData& snap = reg.getSnapshot(msg.snapshotkey());

    // Note this has to be mmapped to be page-aligned
    snapshotMemory = (uint8_t*)mmap(
      nullptr, snap.size, PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    snapshotSize = snap.size;

    reg.mapSnapshot(msg.snapshotkey(), snapshotMemory);
}

std::shared_ptr<Executor> DistTestExecutorFactory::createExecutor(
  const faabric::Message& msg)
{
    return std::make_shared<DistTestExecutor>(msg);
}
}
