#pragma once

#include "DummyExecutor.h"
#include <faabric/executor/FaabricPool.h>

using namespace faabric::executor;

namespace faabric::executor {
class DummyExecutorPool : public FaabricPool
{
  public:
    explicit DummyExecutorPool(int nThreads)
      : FaabricPool(nThreads)
    {}

  protected:
    std::unique_ptr<FaabricExecutor> createExecutor(int threadIdx)
    {
        return std::make_unique<DummyExecutor>(threadIdx);
    }
};
}

