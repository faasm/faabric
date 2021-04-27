#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/InMemoryMessageQueue.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/logging.h>

namespace faabric::executor {
class FaabricExecutor
{
  public:
    explicit FaabricExecutor(int threadIdxIn);

    virtual ~FaabricExecutor() {}

    void bindToFunction(const faabric::Message& msg, bool force = false);

    void run();

    bool isBound();

    virtual std::string processNextMessage();

    void batchExecuteThreads(faabric::scheduler::MessageTask& task);

    std::string executeCall(faabric::Message& call);

    void finish();

    virtual void flush();

    std::string id;

    const int threadIdx;

  protected:
    virtual bool doExecute(faabric::Message& msg);

    virtual int32_t executeThread(
      int threadPoolIdx,
      std::shared_ptr<faabric::BatchExecuteRequest> req,
      faabric::Message& msg);

    virtual void postBind(const faabric::Message& msg, bool force);

    virtual void preFinishCall(faabric::Message& call,
                               bool success,
                               const std::string& errorMsg);

    virtual void postFinishCall();

    virtual void postFinish();

    bool _isBound = false;

    faabric::scheduler::Scheduler& scheduler;

    std::shared_ptr<faabric::scheduler::InMemoryMessageQueue> bindQueue;
    std::shared_ptr<faabric::scheduler::InMemoryBatchQueue> functionQueue;

    int executionCount = 0;

    std::mutex threadsMutex;
    uint32_t threadPoolSize = 0;
    std::unordered_map<int, std::thread> threads;
    std::unordered_map<
      int,
      faabric::util::Queue<
        std::pair<int, std::shared_ptr<faabric::BatchExecuteRequest>>>>
      threadQueues;

  private:
    faabric::Message boundMessage;

    void finishCall(faabric::Message& msg,
                    bool success,
                    const std::string& errorMsg);
};
}
