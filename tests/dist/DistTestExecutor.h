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
    DistTestExecutor(faabric::Message& msg);

    ~DistTestExecutor();

    int32_t executeTask(
      int threadPoolIdx,
      int msgIdx,
      std::shared_ptr<faabric::BatchExecuteRequest> req) override;

    std::shared_ptr<faabric::util::SnapshotData> snapshot() override;

    size_t snapshotSize = 0;

  protected:
    std::shared_ptr<faabric::util::SnapshotData> _snapshot = nullptr;
};

class DistTestExecutorFactory : public faabric::scheduler::ExecutorFactory
{
  protected:
    std::shared_ptr<faabric::scheduler::Executor> createExecutor(
      faabric::Message& msg) override;
};
}
