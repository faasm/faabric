#pragma once

#include <faabric/scheduler/ExecutorFactory.h>

namespace faabric::scheduler {

class DummyExecutorFactory : public ExecutorFactory
{
  protected:
    std::shared_ptr<Executor> createExecutor(
      const faabric::Message& msg) override;
};
}
