#include "DummyExecutor.h"

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/func.h>

namespace faabric::scheduler {

DummyExecutor::DummyExecutor(const faabric::Message& msg)
  : Executor(msg)
{}

DummyExecutor::~DummyExecutor() {}

bool DummyExecutor::doExecute(faabric::Message& call)
{
    auto logger = faabric::util::getLogger();
    logger->debug("DummyExecutor executing function {}", call.id());

    call.set_outputdata(fmt::format("DummyExecutor executed {}", call.id()));

    return true;
}

int32_t DummyExecutor::executeThread(
  int threadPoolIdx,
  std::shared_ptr<faabric::BatchExecuteRequest> req,
  faabric::Message& msg)
{
    auto logger = faabric::util::getLogger();
    logger->debug("DummyExecutor executing thread {}", msg.id());

    return 0;
}
}
