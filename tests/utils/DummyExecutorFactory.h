#pragma once

#include <faabric/executor/ExecutorFactory.h>

namespace faabric::executor {

class DummyExecutorFactory : public ExecutorFactory
{
  public:
    void reset();

    int getFlushCount();

    std::shared_ptr<Executor> createExecutor(faabric::Message& msg) override;

  protected:
    void flushHost() override;

  private:
    int flushCount = 0;
};
}
