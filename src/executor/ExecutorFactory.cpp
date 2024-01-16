#include <faabric/executor/ExecutorFactory.h>
#include <faabric/util/logging.h>

namespace faabric::executor {

static std::shared_ptr<ExecutorFactory> _factory;

void ExecutorFactory::flushHost()
{
    SPDLOG_WARN("Using default flush method");
}

void setExecutorFactory(std::shared_ptr<ExecutorFactory> fac)
{
    _factory = fac;
}

std::shared_ptr<ExecutorFactory> getExecutorFactory()
{
    if (_factory == nullptr) {
        throw std::runtime_error("No executor factory set");
    }

    return _factory;
}
}
