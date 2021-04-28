#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/ExecGraph.h>
#include <faabric/scheduler/InMemoryMessageQueue.h>
#include <faabric/util/config.h>
#include <faabric/util/func.h>
#include <faabric/util/logging.h>
#include <faabric/util/queue.h>

#include <future>
#include <shared_mutex>

#define AVAILABLE_HOST_SET "available_hosts"

namespace faabric::scheduler {

class Scheduler;

class Executor
{
  public:
    explicit Executor(Scheduler& sch, const faabric::Message& msg);

    virtual ~Executor() {}

    void batchExecuteThreads(std::vector<int> msgIdxs,
                             std::shared_ptr<faabric::BatchExecuteRequest> req);

    std::string executeFunction(
      int msgIdx,
      std::shared_ptr<faabric::BatchExecuteRequest> req);

    void finish();

    virtual void flush();

    std::string id;

  protected:
    virtual bool doExecute(faabric::Message& msg);

    virtual int32_t executeThread(
      int threadPoolIdx,
      std::shared_ptr<faabric::BatchExecuteRequest> req,
      faabric::Message& msg);

    virtual void postBind(const faabric::Message& msg);

    virtual void preFinishCall(faabric::Message& call,
                               bool success,
                               const std::string& errorMsg);

    virtual void postFinishCall();

    virtual void postFinish();

    void invokeThreads(std::shared_ptr<faabric::BatchExecuteRequest> req);

    bool _isBound = false;

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
    Scheduler& scheduler;

    void finishCall(faabric::Message& msg,
                    bool success,
                    const std::string& errorMsg);

    void executeTask(int threadPoolIdx,
                     int msgIdx,
                     std::shared_ptr<faabric::BatchExecuteRequest> req,
                     bool isThread);
};

class Scheduler
{
  public:
    Scheduler();

    void callFunction(faabric::Message& msg, bool forceLocal = false);

    std::vector<std::string> callFunctions(
      std::shared_ptr<faabric::BatchExecuteRequest> req,
      bool forceLocal = false);

    void reset();

    void shutdown();

    void broadcastSnapshotDelete(const faabric::Message& msg,
                                 const std::string& snapshotKey);

    long getFunctionInFlightCount(const faabric::Message& msg);

    long getFunctionFaasletCount(const faabric::Message& msg);

    int getFunctionRegisteredHostCount(const faabric::Message& msg);

    std::unordered_set<std::string> getFunctionRegisteredHosts(
      const faabric::Message& msg);

    void broadcastFlush();

    void flushLocally();

    std::string getMessageStatus(unsigned int messageId);

    void setFunctionResult(faabric::Message& msg);

    faabric::Message getFunctionResult(unsigned int messageId, int timeout);

    void setThreadResult(const faabric::Message& msg, int32_t returnValue);

    void setThreadResult(uint32_t msgId, int32_t returnValue);

    int32_t awaitThreadResult(uint32_t messageId);

    std::string getThisHost();

    std::unordered_set<std::string> getAvailableHosts();

    void addHostToGlobalSet();

    void addHostToGlobalSet(const std::string& host);

    void removeHostFromGlobalSet(const std::string& host);

    void removeRegisteredHost(const std::string& host,
                              const faabric::Message& msg);

    faabric::HostResources getThisHostResources();

    void setThisHostResources(faabric::HostResources& res);

    // ----------------------------------
    // Testing
    // ----------------------------------
    std::vector<unsigned int> getRecordedMessagesAll();

    std::vector<unsigned int> getRecordedMessagesLocal();

    std::vector<std::pair<std::string, unsigned int>>
    getRecordedMessagesShared();

    // ----------------------------------
    // Exec graph
    // ----------------------------------
    void logChainedFunction(unsigned int parentMessageId,
                            unsigned int chainedMessageId);

    std::unordered_set<unsigned int> getChainedFunctions(unsigned int msgId);

    ExecGraph getFunctionExecGraph(unsigned int msgId);

    void notifyCallFinished(const faabric::Message& msg);

    void notifyFaasletFinished(const faabric::Message& msg);

  protected:
    virtual std::shared_ptr<Executor> createExecutor(
      const Scheduler& sch,
      const faabric::Message& msg) = 0;

  private:
    std::string thisHost;

    faabric::util::SystemConfig& conf;

    std::unordered_map<std::string, std::vector<std::shared_ptr<Executor>>>
      executingFaaslets;

    std::unordered_map<std::string, std::vector<std::shared_ptr<Executor>>>
      warmFaaslets;

    std::shared_mutex mx;

    std::unordered_map<std::string, long> faasletCounts;
    std::unordered_map<std::string, long> inFlightCounts;

    std::unordered_map<uint32_t, std::promise<int32_t>> threadResults;

    faabric::HostResources thisHostResources;
    std::unordered_map<std::string, std::unordered_set<std::string>>
      registeredHosts;

    std::vector<unsigned int> recordedMessagesAll;
    std::vector<unsigned int> recordedMessagesLocal;
    std::vector<std::pair<std::string, unsigned int>> recordedMessagesShared;

    std::shared_ptr<Executor> claimFaaslet(const std::string& appId);

    void returnFaaslet(const std::string& appId,
                       std::shared_ptr<Executor> faaslet);

    void addFaaslets(const faabric::Message& msg);

    void addFaasletsForBatch(const faabric::Message& msg);

    void doAddFaaslets(const faabric::Message& msg, int count);

    faabric::HostResources getHostResources(const std::string& host);

    void incrementInFlightCount(const faabric::Message& msg, int count);

    ExecGraphNode getFunctionExecGraphNode(unsigned int msgId);

    void registerThread(uint32_t msgId);

    int scheduleFunctionsOnHost(
      const std::string& host,
      std::shared_ptr<faabric::BatchExecuteRequest> req,
      std::vector<std::string>& records,
      int offset);
};

extern Scheduler& getScheduler();

}
