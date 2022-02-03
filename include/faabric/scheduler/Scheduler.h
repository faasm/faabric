#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/ExecGraph.h>
#include <faabric/scheduler/FunctionCallClient.h>
#include <faabric/scheduler/FunctionMigrationThread.h>
#include <faabric/scheduler/InMemoryMessageQueue.h>
#include <faabric/snapshot/SnapshotClient.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/transport/PointToPointBroker.h>
#include <faabric/util/config.h>
#include <faabric/util/dirty.h>
#include <faabric/util/func.h>
#include <faabric/util/queue.h>
#include <faabric/util/scheduling.h>
#include <faabric/util/snapshot.h>
#include <faabric/util/timing.h>

#include <future>
#include <shared_mutex>

#define AVAILABLE_HOST_SET "available_hosts"

namespace faabric::scheduler {

typedef std::pair<std::shared_ptr<BatchExecuteRequest>,
                  std::shared_ptr<faabric::util::SchedulingDecision>>
  InFlightPair;

class Scheduler;

Scheduler& getScheduler();

class ExecutorTask
{
  public:
    ExecutorTask() = default;

    ExecutorTask(int messageIndexIn,
                 std::shared_ptr<faabric::BatchExecuteRequest> reqIn,
                 std::shared_ptr<std::atomic<int>> batchCounterIn,
                 bool skipResetIn);

    std::shared_ptr<faabric::BatchExecuteRequest> req;
    std::shared_ptr<std::atomic<int>> batchCounter;
    int messageIndex = 0;
    bool skipReset = false;
};

class Executor
{
  public:
    std::string id;

    explicit Executor(faabric::Message& msg);

    virtual ~Executor() = default;

    std::vector<std::pair<uint32_t, int32_t>> executeThreads(
      std::shared_ptr<faabric::BatchExecuteRequest> req,
      const std::vector<faabric::util::SnapshotMergeRegion>& mergeRegions);

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

    std::shared_ptr<faabric::util::SnapshotData> getMainThreadSnapshot(
      faabric::Message& msg,
      bool createIfNotExists = false);

    virtual std::span<uint8_t> getMemoryView();

  protected:
    virtual void restore(const std::string& snapshotKey);

    virtual void postFinish();

    virtual void setMemorySize(size_t newSize);

    faabric::Message boundMessage;

    Scheduler& sch;

    faabric::snapshot::SnapshotRegistry& reg;

    faabric::util::DirtyTracker& tracker;

    uint32_t threadPoolSize = 0;

  private:
    std::atomic<bool> claimed = false;

    // ---- Application threads ----
    std::shared_mutex threadExecutionMutex;
    std::unordered_map<std::string, int> cachedGroupIds;
    std::unordered_map<std::string, std::vector<std::string>>
      cachedDecisionHosts;
    std::vector<std::pair<uint32_t, uint32_t>> dirtyRegions;

    void deleteMainThreadSnapshot(const faabric::Message& msg);

    // ---- Function execution thread pool ----
    std::mutex threadsMutex;
    std::vector<std::shared_ptr<std::thread>> threadPoolThreads;
    std::vector<std::shared_ptr<std::thread>> deadThreads;
    std::set<int> availablePoolThreads;

    std::vector<faabric::util::Queue<ExecutorTask>> threadTaskQueues;

    void threadPoolThread(int threadPoolIdx);
};

Executor* getExecutingExecutor();

void setExecutingExecutor(Executor* exec);

class Scheduler
{
  public:
    Scheduler();

    void callFunction(faabric::Message& msg, bool forceLocal = false);

    faabric::util::SchedulingDecision callFunctions(
      std::shared_ptr<faabric::BatchExecuteRequest> req);

    faabric::util::SchedulingDecision callFunctions(
      std::shared_ptr<faabric::BatchExecuteRequest> req,
      faabric::util::SchedulingDecision& hint);

    void reset();

    void resetThreadLocalCache();

    void shutdown();

    void broadcastSnapshotDelete(const faabric::Message& msg,
                                 const std::string& snapshotKey);

    long getFunctionExecutorCount(const faabric::Message& msg);

    int getFunctionRegisteredHostCount(const faabric::Message& msg);

    std::set<std::string> getFunctionRegisteredHosts(
      const faabric::Message& msg,
      bool acquireLock = true);

    void broadcastFlush();

    void flushLocally();

    void setFunctionResult(faabric::Message& msg);

    faabric::Message getFunctionResult(unsigned int messageId, int timeout);

    void setThreadResult(const faabric::Message& msg, int32_t returnValue);

    void pushSnapshotDiffs(
      const faabric::Message& msg,
      const std::string& snapshotKey,
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

    // ----------------------------------
    // Function Migration
    // ----------------------------------
    void checkForMigrationOpportunities();

    std::shared_ptr<faabric::PendingMigrations> getPendingAppMigrations(
      uint32_t appId);

    void addPendingMigration(std::shared_ptr<faabric::PendingMigrations> msg);

    void removePendingMigration(uint32_t appId);

    // ----------------------------------
    // Clients
    // ----------------------------------
    faabric::scheduler::FunctionCallClient& getFunctionCallClient(
      const std::string& otherHost);

    faabric::snapshot::SnapshotClient& getSnapshotClient(
      const std::string& otherHost);

  private:
    std::string thisHost;

    faabric::util::SystemConfig& conf;

    std::shared_mutex mx;

    // ---- Executors ----
    std::vector<std::shared_ptr<Executor>> deadExecutors;

    std::unordered_map<std::string, std::vector<std::shared_ptr<Executor>>>
      executors;

    // ---- Threads ----
    std::unordered_map<uint32_t, std::promise<int32_t>> threadResults;

    std::unordered_map<uint32_t,
                       std::promise<std::unique_ptr<faabric::Message>>>
      localResults;

    std::unordered_map<std::string, std::set<std::string>> pushedSnapshotsMap;

    std::mutex localResultsMutex;

    // ---- Host resources and hosts ----
    faabric::HostResources thisHostResources;
    std::atomic<int32_t> thisHostUsedSlots = 0;

    void updateHostResources();

    faabric::HostResources getHostResources(const std::string& host);

    // ---- Actual scheduling ----
    std::set<std::string> availableHostsCache;

    std::unordered_map<std::string, std::set<std::string>> registeredHosts;

    faabric::util::SchedulingDecision makeSchedulingDecision(
      std::shared_ptr<faabric::BatchExecuteRequest> req,
      faabric::util::SchedulingTopologyHint topologyHint);

    faabric::util::SchedulingDecision doCallFunctions(
      std::shared_ptr<faabric::BatchExecuteRequest> req,
      faabric::util::SchedulingDecision& decision,
      faabric::util::FullLock& lock);

    std::shared_ptr<Executor> claimExecutor(
      faabric::Message& msg,
      faabric::util::FullLock& schedulerLock);

    std::vector<std::string> getUnregisteredHosts(const std::string& funcStr,
                                                  bool noCache = false);

    // ---- Accounting and debugging ----
    std::vector<faabric::Message> recordedMessagesAll;
    std::vector<faabric::Message> recordedMessagesLocal;
    std::vector<std::pair<std::string, faabric::Message>>
      recordedMessagesShared;

    ExecGraphNode getFunctionExecGraphNode(unsigned int msgId);

    // ---- Point-to-point ----
    faabric::transport::PointToPointBroker& broker;

    // ---- Function migration ----
    FunctionMigrationThread functionMigrationThread;
    std::unordered_map<uint32_t, InFlightPair> inFlightRequests;
    std::unordered_map<uint32_t, std::shared_ptr<faabric::PendingMigrations>>
      pendingMigrations;

    std::vector<std::shared_ptr<faabric::PendingMigrations>>
    doCheckForMigrationOpportunities(
      faabric::util::MigrationStrategy migrationStrategy =
        faabric::util::MigrationStrategy::BIN_PACK);

    void broadcastPendingMigrations(
      std::shared_ptr<faabric::PendingMigrations> pendingMigrations);

    void doStartFunctionMigrationThread(
      std::shared_ptr<faabric::BatchExecuteRequest> req,
      faabric::util::SchedulingDecision& decision);
};

}
