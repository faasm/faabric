#include <faabric/executor/DummyExecutor.h>

namespace faabric::executor {

DummyExecutor::DummyExecutor(int threadIdxIn)
  : FaabricExecutor(threadIdxIn)
{}

void DummyExecutor::flush() {}

void DummyExecutor::postBind(const faabric::Message& msg, bool force) {}

bool DummyExecutor::doExecute(faabric::Message& call) {}

std::future<int32_t> DummyExecutor::doBatchExecuteThread(
  int threadPoolIdx,
  const faabric::Message& msg)
{}

void DummyExecutor::preFinishCall(faabric::Message& call,
                                  bool success,
                                  const std::string& errorMsg)
{}

void DummyExecutor::postFinish() {}
}
