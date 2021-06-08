#pragma once

#include <faabric/endpoint/FaabricEndpoint.h>
#include <faabric/scheduler/ExecutorFactory.h>
#include <faabric/scheduler/Scheduler.h>

using namespace faabric::scheduler;

namespace faabric::mpi_native {
class MpiExecutor final : public Executor
{
  public:
    explicit MpiExecutor(const faabric::Message& msg);

    int32_t executeTask(
      int threadPoolIdx,
      int msgIdx,
      std::shared_ptr<faabric::BatchExecuteRequest> req) override;
};

class MpiExecutorFactory : public ExecutorFactory
{
  protected:
    std::shared_ptr<Executor> createExecutor(
      const faabric::Message& msg) override
    {
        return std::make_unique<MpiExecutor>(msg);
    }
};

int mpiNativeMain(int argc, char** argv);

extern faabric::Message* executingCall;
extern int __attribute__((weak)) mpiFunc();
}
