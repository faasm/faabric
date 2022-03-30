#pragma once

#include <faabric/scheduler/Scheduler.h>

namespace faabric::scheduler {

class DummyExecutor final : public Executor
{
  public:
    DummyExecutor(faabric::Message& msg);
  protected:
    int32_t executeTask(
      int threadPoolIdx,
      int msgIdx,
      std::shared_ptr<faabric::BatchExecuteRequest> req) override;
};

}
