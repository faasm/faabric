#include "DummyExecutor.h"

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/func.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>

#define SHORT_SLEEP_MS 50

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

    // Make sure the executor stays busy and cannot accept another task while
    // the scheduler is executing its logic. TSan tests are sensitive to this.
    SLEEP_MS(SHORT_SLEEP_MS);

    return 0;
}
}
