#include <faabric/scheduler/DummyExecutor.h>
#include <faabric/scheduler/DummyExecutorFactory.h>

namespace faabric::scheduler {

std::shared_ptr<Executor> DummyExecutorFactory::createExecutor(
  const faabric::Message& msg)
{
    return std::make_shared<DummyExecutor>(msg);
}
}
