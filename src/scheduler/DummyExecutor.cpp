#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/DummyExecutor.h>
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

    if (call.function() == "thread-check") {
        call.set_outputdata(
          fmt::format("Threaded function {} executed successfully", call.id()));

        // Set up the request
        int nThreads = std::stoi(call.inputdata());
        std::shared_ptr<faabric::BatchExecuteRequest> req =
          faabric::util::batchExecFactory("dummy", "thread-check", nThreads);

        for (faabric::Message& m : *req->mutable_messages()) {
            m.set_snapshotkey(call.snapshotkey());
        }

        // Call the threads
        invokeThreads(req);
    } else if (call.function() == "simple") {
        call.set_outputdata(
          fmt::format("Simple function {} executed successfully", call.id()));
    } else {
        logger->error(
          "Dummy function {}/{} not recognised", call.user(), call.function());
        return false;
    }

    return true;
}

int32_t DummyExecutor::executeThread(
  int threadPoolIdx,
  std::shared_ptr<faabric::BatchExecuteRequest> req,
  faabric::Message& msg)
{
    auto logger = faabric::util::getLogger();
    logger->debug("DummyExecutor executing thread {}", msg.id());

    return msg.id() / 100;
}
}
