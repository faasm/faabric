#include "DistTestExecutor.h"
#include "faabric/util/snapshot.h"

#include <sys/mman.h>

#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/util/logging.h>

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

DistTestExecutor::DistTestExecutor(faabric::Message& msg)
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

std::shared_ptr<faabric::util::SnapshotData> DistTestExecutor::snapshot()
{
    if (_snapshot == nullptr) {
        _snapshot = std::make_shared<faabric::util::SnapshotData>(snapshotSize);
    }

    return _snapshot;
}

std::shared_ptr<Executor> DistTestExecutorFactory::createExecutor(
  faabric::Message& msg)
{
    return std::make_shared<DistTestExecutor>(msg);
}
}
