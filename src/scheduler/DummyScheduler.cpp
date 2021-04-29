#pragma once

#include <faabric/scheduler/DummyExecutor.h>
#include <faabric/scheduler/DummyScheduler.h>
#include <faabric/scheduler/Scheduler.h>

namespace faabric::scheduler {

std::shared_ptr<Executor> DummyScheduler::createExecutor(
  const faabric::Message& msg)
{
    return std::make_unique<DummyExecutor>(*this, msg);
}
}
