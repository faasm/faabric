#include <faabric/scheduler/ExecutorFactory.h>

namespace faabric::scheduler {

static std::shared_ptr<ExecutorFactory> _fac;

void setExecutorFactory(std::shared_ptr<ExecutorFactory> fac)
{
    _fac = fac;
}

std::shared_ptr<ExecutorFactory> getExecutorFactory()
{
    if (_fac == nullptr) {
        throw std::runtime_error("No executor factory set");
    }

    return _fac;
}
}
