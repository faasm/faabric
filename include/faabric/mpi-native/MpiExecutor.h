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
};

class SingletonPool : public faabric::executor::FaabricPool
{
  public:
    SingletonPool();

    ~SingletonPool();

    void startPool();

  protected:
    std::unique_ptr<FaabricExecutor> createExecutor(int threadIdx) override
    {
        return std::make_unique<MpiExecutor>();
    }

  private:
    faabric::scheduler::Scheduler& scheduler;
    faabric::endpoint::FaabricEndpoint endpoint;
};

extern faabric::Message* executingCall;
}
