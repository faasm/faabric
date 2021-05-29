#pragma once

#include <faabric/scheduler/ExecutorFactory.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/config.h>
#include <faabric/util/func.h>

namespace tests {

typedef int (*ExecutorFunction)(
  faabric::scheduler::Executor* exec,
  int threadPoolIdx,
  int msgIdx,
  std::shared_ptr<faabric::BatchExecuteRequest> req);

void registerDistTestExecutorCallback(const char* user,
                                      const char* funcName,
                                      ExecutorFunction func);

class DistTestExecutor final : public faabric::scheduler::Executor
{
  public:
    DistTestExecutor(const faabric::Message& msg);

    ~DistTestExecutor();

    int32_t executeTask(
      int threadPoolIdx,
      int msgIdx,
      std::shared_ptr<faabric::BatchExecuteRequest> req) override;

    faabric::util::SnapshotData snapshot() override;

    uint8_t* snapshotMemory = nullptr;
    size_t snapshotSize = 0;

  protected:
    void restore(const faabric::Message& msg) override;
};

class DistTestExecutorFactory : public faabric::scheduler::ExecutorFactory
{
  protected:
    std::shared_ptr<faabric::scheduler::Executor> createExecutor(
      const faabric::Message& msg) override;
};
}
