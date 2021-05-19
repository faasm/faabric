#include "DistTestExecutor.h"

using namespace faabric::scheduler;

typedef int (*ExecutorFunction)(
  int threadPoolIdx,
  int msgIdx,
  std::shared_ptr<faabric::BatchExecuteRequest> req);

std::unordered_map<std::string, ExecutorFunction> executorFunctions;

namespace tests {

void registerDistTestExecutorCallback(const char* user,
                                      const char* funcName,
                                      ExecutorFunction func)
{
    std::string key = std::string(user) + "_" + std::string(funcName);
    executorFunctions[key] = func;

    const auto& logger = faabric::util::getLogger();
    logger->debug("Registered executor callback for {}", key);
}

ExecutorFunction getDistTestExecutorCallback(const faabric::Message& msg)
{
    std::string key = msg.user() + "_" + msg.function();
    if (executorFunctions.find(key) == executorFunctions.end()) {
        const auto& logger = faabric::util::getLogger();
        logger->error("No registered executor callback for {}", key);
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
    callback(threadPoolIdx, msgIdx, req);

    return 0;
}

std::shared_ptr<Executor> DistTestExecutorFactory::createExecutor(
  const faabric::Message& msg)
{
    return std::make_shared<DistTestExecutor>(msg);
}
}
