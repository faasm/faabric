#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/ExecGraph.h>
#include <faabric/scheduler/FunctionCallClient.h>
#include <faabric/scheduler/InMemoryMessageQueue.h>
#include <faabric/snapshot/SnapshotClient.h>
#include <faabric/util/config.h>
#include <faabric/util/func.h>
#include <faabric/util/queue.h>
#include <faabric/util/snapshot.h>
#include <faabric/util/timing.h>

#include <future>
#include <shared_mutex>

#define AVAILABLE_HOST_SET "available_hosts"

namespace faabric::scheduler {

class Scheduler;

Scheduler& getScheduler();

class ExecutorTask
{
  public:
    ExecutorTask(int messageIndexIn,
                 std::shared_ptr<faabric::BatchExecuteRequest> reqIn,
                 std::shared_ptr<std::atomic<int>> batchCounterIn,
                 bool needsSnapshotPushIn);

    int messageIndex = 0;
    std::shared_ptr<faabric::BatchExecuteRequest> req;
    std::shared_ptr<std::atomic<int>> batchCounter;
    bool needsSnapshotPush = false;
};

class Executor
{
  public:
    std::string id;

    explicit Executor(faabric::Message& msg);

    virtual ~Executor();

    void executeTasks(std::vector<int> msgIdxs,
                      std::shared_ptr<faabric::BatchExecuteRequest> req);

    void finish();

    virtual void reset(faabric::Message& msg);

    virtual int32_t executeTask(
      int threadPoolIdx,
      int msgIdx,
      std::shared_ptr<faabric::BatchExecuteRequest> req);

    bool tryClaim();

    void claim();

    void releaseClaim();

    virtual faabric::util::SnapshotData snapshot();

  protected:
    virtual void restore(faabric::Message& msg);

    virtual void postFinish();

    faabric::Message boundMessage;

    uint32_t threadPoolSize = 0;

  private:
    std::string lastSnapshot;

    std::atomic<bool> claimed = false;

    std::mutex threadsMutex;
    std::vector<std::shared_ptr<std::thread>> threadPoolThreads;
    std::vector<std::shared_ptr<std::thread>> deadThreads;

    std::vector<faabric::util::Queue<ExecutorTask>> threadQueues;

    void threadPoolThread(int threadPoolIdx);
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

    void resetThreadLocalCache();

    void shutdown();

    void broadcastSnapshotDelete(const faabric::Message& msg,
                                 const std::string& snapshotKey);

    long getFunctionExecutorCount(const faabric::Message& msg);

    int getFunctionRegisteredHostCount(const faabric::Message& msg);

    std::set<std::string> getFunctionRegisteredHosts(
      const faabric::Message& msg);

    void broadcastFlush();

    void flushLocally();

    std::string getMessageStatus(unsigned int messageId);

    void setFunctionResult(faabric::Message& msg);

    faabric::Message getFunctionResult(unsigned int messageId, int timeout);

    void setThreadResult(const faabric::Message& msg, int32_t returnValue);

    void pushSnapshotDiffs(
      const faabric::Message& msg,
      const std::vector<faabric::util::SnapshotDiff>& diffs);

    void setThreadResultLocally(uint32_t msgId, int32_t returnValue);

    int32_t awaitThreadResult(uint32_t messageId);

    void registerThread(uint32_t msgId);

    void vacateSlot();

    void notifyExecutorShutdown(Executor* exec, const faabric::Message& msg);

    std::string getThisHost();

    std::set<std::string> getAvailableHosts();

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
    std::vector<faabric::Message> getRecordedMessagesAll();

    std::vector<faabric::Message> getRecordedMessagesLocal();

    std::vector<std::pair<std::string, faabric::Message>>
    getRecordedMessagesShared();

    void clearRecordedMessages();

    // ----------------------------------
    // Exec graph
    // ----------------------------------
    void logChainedFunction(unsigned int parentMessageId,
                            unsigned int chainedMessageId);

    std::set<unsigned int> getChainedFunctions(unsigned int msgId);

    ExecGraph getFunctionExecGraph(unsigned int msgId);

  private:
    std::string thisHost;

    faabric::util::SystemConfig& conf;

    std::vector<std::shared_ptr<Executor>> deadExecutors;

    std::unordered_map<std::string, std::vector<std::shared_ptr<Executor>>>
      executors;

    std::shared_mutex mx;

    std::unordered_map<uint32_t, std::promise<int32_t>> threadResults;

    faabric::scheduler::FunctionCallClient& getFunctionCallClient(
      const std::string& otherHost);

    faabric::snapshot::SnapshotClient& getSnapshotClient(
      const std::string& otherHost);

    faabric::HostResources thisHostResources;
    std::set<std::string> availableHostsCache;
    std::unordered_map<std::string, std::set<std::string>> registeredHosts;

    std::vector<faabric::Message> recordedMessagesAll;
    std::vector<faabric::Message> recordedMessagesLocal;
    std::vector<std::pair<std::string, faabric::Message>>
      recordedMessagesShared;

    std::vector<std::string> getUnregisteredHosts(const std::string& funcStr,
                                                  bool noCache = false);

    std::shared_ptr<Executor> claimExecutor(faabric::Message& msg);

    faabric::HostResources getHostResources(const std::string& host);

    ExecGraphNode getFunctionExecGraphNode(unsigned int msgId);

    void updateHostResources();

    int scheduleFunctionsOnHost(
      const std::string& host,
      std::shared_ptr<faabric::BatchExecuteRequest> req,
      std::vector<std::string>& records,
      int offset,
      faabric::util::SnapshotData* snapshot);
};

}
