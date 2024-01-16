#pragma once

#include <faabric/executor/ExecutorFactory.h>
#include <faabric/util/config.h>
#include <faabric/util/func.h>

namespace tests {

#define DIST_TEST_EXECUTOR_MEMORY_SIZE (30 * faabric::util::HOST_PAGE_SIZE)

class DistTestExecutor final : public faabric::executor::Executor
{
  public:
    DistTestExecutor(faabric::Message& msg);

    ~DistTestExecutor();

    int32_t executeTask(
      int threadPoolIdx,
      int msgIdx,
      std::shared_ptr<faabric::BatchExecuteRequest> req) override;

    void reset(faabric::Message& msg) override;

    std::span<uint8_t> getMemoryView() override;

    std::span<uint8_t> getDummyMemory();

    // Helper method to execute threads in a distributed test
    std::vector<std::pair<uint32_t, int32_t>> executeThreads(
      std::shared_ptr<faabric::BatchExecuteRequest> req,
      const std::vector<faabric::util::SnapshotMergeRegion>& mergeRegions);

  protected:
    void setMemorySize(size_t newSize) override;

  private:
    faabric::util::MemoryRegion dummyMemory = nullptr;

    size_t dummyMemorySize = DIST_TEST_EXECUTOR_MEMORY_SIZE;

    void setUpDummyMemory(size_t memSize);
};

class DistTestExecutorFactory : public faabric::executor::ExecutorFactory
{
  protected:
    std::shared_ptr<faabric::executor::Executor> createExecutor(
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
