#include <faabric/scheduler/ExecutorFactory.h>

namespace faabric::scheduler {

static std::shared_ptr<ExecutorFactory> _factory;

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
