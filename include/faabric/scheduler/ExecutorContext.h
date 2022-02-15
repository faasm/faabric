#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/Scheduler.h>

namespace faabric::scheduler {

/**
 * Globally-accessible wrapper that allows executing applications to query
 * their execution context. The context is thread-local, so applications can
 * query which specific message they are executing.
 */
class ExecutorContext
{
  public:
    ExecutorContext(Executor* executorIn,
                    std::shared_ptr<faabric::BatchExecuteRequest> reqIn,
                    int msgIdx);

    static bool isSet();

    static void set(Executor* executorIn,
                    std::shared_ptr<faabric::BatchExecuteRequest> reqIn,
                    int msgIdxIn);

    static void unset();

    static std::shared_ptr<ExecutorContext> get();

    Executor* getExecutor() { return executor; }

    std::shared_ptr<faabric::BatchExecuteRequest> getBatchRequest()
    {
        return req;
    }

    faabric::Message& getMsg()
    {
        if (req == nullptr) {
            throw std::runtime_error(
              "Getting message when no request set in context");
        }
        return req->mutable_messages()->at(msgIdx);
    }

    int getMsgIdx() { return msgIdx; }

  private:
    Executor* executor = nullptr;
    std::shared_ptr<faabric::BatchExecuteRequest> req = nullptr;
    int msgIdx = 0;
};
}
