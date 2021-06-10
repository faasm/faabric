#include "DummyExecutor.h"

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/func.h>
#include <faabric/util/logging.h>

namespace faabric::scheduler {

DummyExecutor::DummyExecutor(faabric::Message& msg)
  : Executor(msg)
{}

DummyExecutor::~DummyExecutor() {}

int32_t DummyExecutor::executeTask(
  int threadPoolIdx,
  int msgIdx,
  std::shared_ptr<faabric::BatchExecuteRequest> req)
{

    faabric::Message& msg = req->mutable_messages()->at(msgIdx);
    SPDLOG_DEBUG("DummyExecutor executing task {}", msg.id());

    msg.set_outputdata(fmt::format("DummyExecutor executed {}", msg.id()));

    return 0;
}
}
