#pragma once

#include <faabric/scheduler/ExecutorFactory.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/config.h>
#include <faabric/util/func.h>

namespace tests {

class DistTestExecutor final : public faabric::scheduler::Executor
{
  public:
    DistTestExecutor(faabric::Message& msg);

    ~DistTestExecutor();

    int32_t executeTask(
      int threadPoolIdx,
      int msgIdx,
      std::shared_ptr<faabric::BatchExecuteRequest> req) override;

    void reset(faabric::Message& msg) override;

    void restore(const std::string& snapshotKey) override;

    std::span<uint8_t> getMemoryView() override;

    std::span<uint8_t> getDummyMemory();

    void setUpDummyMemory(size_t memSize);

  private:
    faabric::util::MemoryRegion dummyMemory = nullptr;

    size_t dummyMemorySize = 0;
};

class DistTestExecutorFactory : public faabric::scheduler::ExecutorFactory
{
  protected:
    std::shared_ptr<faabric::scheduler::Executor> createExecutor(
      faabric::Message& msg) override;
};

typedef int (*ExecutorFunction)(
  tests::DistTestExecutor* exec,
  int threadPoolIdx,
  int msgIdx,
  std::shared_ptr<faabric::BatchExecuteRequest> req);

void registerDistTestExecutorCallback(const char* user,
                                      const char* funcName,
                                      ExecutorFunction func);

}
