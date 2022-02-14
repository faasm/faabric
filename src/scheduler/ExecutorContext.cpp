#include <faabric/scheduler/ExecutorContext.h>

namespace faabric::scheduler {

static thread_local std::shared_ptr<ExecutorContext> context = nullptr;

ExecutorContext::ExecutorContext(
  Executor* executorIn,
  std::shared_ptr<faabric::BatchExecuteRequest> reqIn,
  int msgIdxIn)
  : executor(executorIn)
  , req(reqIn)
  , msgIdx(msgIdxIn)
{}

void ExecutorContext::set(Executor* executorIn,
                          std::shared_ptr<faabric::BatchExecuteRequest> reqIn,
                          int appIdxIn)
{
    context = std::make_shared<ExecutorContext>(executorIn, reqIn, appIdxIn);
}

std::shared_ptr<ExecutorContext> ExecutorContext::get()
{
    return context;
}
}
