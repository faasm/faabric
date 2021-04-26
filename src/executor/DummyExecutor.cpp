#include <faabric/executor/DummyExecutor.h>

namespace faabric::executor {

DummyExecutor::DummyExecutor(int threadIdxIn)
  : FaabricExecutor(threadIdxIn)
{}

void DummyExecutor::flush() {}

void DummyExecutor::postBind(const faabric::Message& msg, bool force) {}

bool DummyExecutor::doExecute(faabric::Message& call)
{
    auto logger = faabric::util::getLogger();
    logger->debug("DummyExecutor executing call {}", call.id());
    call.set_outputdata(
      fmt::format("Function {} executed successfully", call.id()));

    return true;
}

int32_t DummyExecutor::executeThread(int threadPoolIdx, faabric::Message& msg)
{
    auto logger = faabric::util::getLogger();
    logger->debug("DummyExecutor executing thread {}", msg.id());

    return msg.id() * 2;
}

void DummyExecutor::preFinishCall(faabric::Message& call,
                                  bool success,
                                  const std::string& errorMsg)
{}

void DummyExecutor::postFinish() {}
}
