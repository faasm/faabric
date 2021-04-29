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
        int nThreads = 5;
        if (!call.inputdata().empty()) {
            nThreads = std::stoi(call.inputdata());
        }

        std::shared_ptr<faabric::BatchExecuteRequest> req =
          faabric::util::batchExecFactory("dummy", "thread-check", nThreads);
        req->set_type(faabric::BatchExecuteRequest::THREADS);

        for (int i = 0; i < req->messages_size(); i++) {
            faabric::Message& m = req->mutable_messages()->at(i);
            m.set_snapshotkey(call.snapshotkey());
            m.set_appindex(i + 1);
        }

        // Call the threads
        Scheduler& sch = getScheduler();
        sch.callFunctions(req);

        for (auto& m : req->messages()) {
            sch.awaitThreadResult(m.id());
        }
    } else {
        call.set_outputdata(
          fmt::format("Simple function {} executed successfully", call.id()));
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
