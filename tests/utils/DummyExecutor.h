#pragma once

#include <faabric/executor/Executor.h>

namespace faabric::executor {

class DummyExecutor final : public Executor
{
  public:
    DummyExecutor(faabric::Message& msg);

  protected:
    int32_t executeTask(
      int threadPoolIdx,
      int msgIdx,
      std::shared_ptr<faabric::BatchExecuteRequest> req) override;

    std::span<uint8_t> getMemoryView() override;
};

}
