#pragma once

#include <faabric/scheduler/Scheduler.h>

namespace faabric::scheduler {

class ExecutorFactory
{
  public:
    virtual ~ExecutorFactory(){};

    virtual std::shared_ptr<Executor> createExecutor(faabric::Message& msg) = 0;

    virtual void flushHost();
};

void setExecutorFactory(std::shared_ptr<ExecutorFactory> fac);

std::shared_ptr<ExecutorFactory> getExecutorFactory();
}
