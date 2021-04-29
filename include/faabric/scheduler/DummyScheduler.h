#pragma once

#include <faabric/scheduler/Scheduler.h>

namespace faabric::scheduler {

class DummyScheduler : public Scheduler
{
  protected:
    std::shared_ptr<Executor> createExecutor(
      const faabric::Message& msg) override;
};
}
