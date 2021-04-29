#pragma once

#include <faabric/scheduler/Scheduler.h>

namespace faabric::scheduler {

class DummyExecutor final : public Executor
{
  public:
    DummyExecutor(const faabric::Message& msg);

  protected:
    bool doExecute(faabric::Message& call) override;

    int32_t executeThread(int threadPoolIdx,
                          std::shared_ptr<faabric::BatchExecuteRequest> req,
                          faabric::Message& msg) override;
};

}
