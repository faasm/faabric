#pragma once

#include <future>

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/InMemoryMessageQueue.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/logging.h>

typedef std::tuple<std::promise<int32_t>,
                   int,
                   std::shared_ptr<faabric::BatchExecuteRequest>>
  ThreadTask;
typedef faabric::util::Queue<ThreadTask> ThreadTaskQueue;
typedef std::unordered_map<int, ThreadTaskQueue> ThreadQueueMap;
typedef std::unordered_map<int, std::thread> ThreadMap;

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

    std::vector<std::future<int32_t>> batchExecuteThreads(
      std::shared_ptr<faabric::BatchExecuteRequest> req);

    std::string executeCall(faabric::Message& call);

    void finish();

    virtual void flush();

    std::string id;

    const int threadIdx;

  protected:
    virtual bool doExecute(faabric::Message& msg);

    virtual int32_t executeThread(int threadPoolIdx,
                                  const faabric::Message& msg);

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
    ThreadQueueMap threadQueues;
    ThreadMap threads;

  private:
    faabric::Message boundMessage;

    void finishCall(faabric::Message& msg,
                    bool success,
                    const std::string& errorMsg);
};
}
