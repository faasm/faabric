#include "DummyExecutorFactory.h"
#include "DummyExecutor.h"

namespace faabric::scheduler {

std::shared_ptr<Executor> DummyExecutorFactory::createExecutor(
  const faabric::Message& msg)
{
    return std::make_shared<DummyExecutor>(msg);
}
}
