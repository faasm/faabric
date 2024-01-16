#include <faabric/executor/ExecutorTask.h>

namespace faabric::executor {

ExecutorTask::ExecutorTask(int messageIndexIn,
                           std::shared_ptr<faabric::BatchExecuteRequest> reqIn)
  : req(std::move(reqIn))
  , messageIndex(messageIndexIn)
{}
}
