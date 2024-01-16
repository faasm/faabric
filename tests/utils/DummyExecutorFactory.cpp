#include "DummyExecutorFactory.h"
#include "DummyExecutor.h"

#include <faabric/util/logging.h>

namespace faabric::executor {

std::shared_ptr<Executor> DummyExecutorFactory::createExecutor(
  faabric::Message& msg)
{
    return std::make_shared<DummyExecutor>(msg);
}

int DummyExecutorFactory::getFlushCount()
{
    return flushCount;
}

void DummyExecutorFactory::flushHost()
{
    flushCount++;
}

void DummyExecutorFactory::reset()
{
    flushCount = 0;
}
}
