#pragma once

#include <faabric/scheduler/Scheduler.h>

namespace faabric::scheduler {

class DummyExecutor final : public Executor
{
  public:
    DummyExecutor(const faabric::Message& msg);

    ~DummyExecutor() override;

  protected:
    int32_t executeTask(
      int threadPoolIdx,
      int msgIdx,
      std::shared_ptr<faabric::BatchExecuteRequest> req) override;
};

}
