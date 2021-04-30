#pragma once

#include <faabric/endpoint/FaabricEndpoint.h>
#include <faabric/scheduler/ExecutorFactory.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/logging.h>

using namespace faabric::scheduler;

namespace faabric::mpi_native {
class MpiExecutor final : public Executor
{
  public:
    explicit MpiExecutor(const faabric::Message& msg);

    virtual ~MpiExecutor();

    bool doExecute(faabric::Message& msg) override;

    void postFinishCall() override;

    void postFinish() override;
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
