#pragma once

#include <faabric/scheduler/ExecutorFactory.h>

namespace faabric::scheduler {

class DummyExecutorFactory : public ExecutorFactory
{
  protected:
    std::shared_ptr<Executor> createExecutor(faabric::Message& msg) override;
};
}
