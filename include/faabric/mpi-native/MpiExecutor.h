#pragma once

#include <faabric/endpoint/FaabricEndpoint.h>
#include <faabric/executor/FaabricExecutor.h>
#include <faabric/executor/FaabricPool.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/logging.h>

namespace faabric::executor {
class MpiExecutor final : public faabric::executor::FaabricExecutor
{
  public:
    explicit MpiExecutor();

    bool doExecute(faabric::Message& msg) override;

    void postFinishCall() override;

    void postFinish() override;
};

class SingletonPool : public faabric::executor::FaabricPool
{
  public:
    SingletonPool();

    ~SingletonPool();

    void startPool(bool background);

  protected:
    std::unique_ptr<FaabricExecutor> createExecutor(int threadIdx) override
    {
        return std::make_unique<MpiExecutor>();
    }

  private:
    faabric::endpoint::FaabricEndpoint endpoint;
    faabric::scheduler::Scheduler& scheduler;
};

extern faabric::Message* executingCall;
extern int __attribute__((weak)) mpiFunc();
}
