#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/ExecGraph.h>
#include <faabric/scheduler/FunctionCallClient.h>
#include <faabric/scheduler/InMemoryMessageQueue.h>
#include <faabric/snapshot/SnapshotClient.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/transport/PointToPointBroker.h>
#include <faabric/util/PeriodicBackgroundThread.h>
#include <faabric/util/asio.h>
#include <faabric/util/clock.h>
#include <faabric/util/config.h>
#include <faabric/util/dirty.h>
#include <faabric/util/func.h>
#include <faabric/util/memory.h>
#include <faabric/util/queue.h>
#include <faabric/util/scheduling.h>
#include <faabric/util/snapshot.h>
#include <faabric/util/timing.h>

#include <future>
#include <shared_mutex>

#define AVAILABLE_HOST_SET "available_hosts"
#define MIGRATED_FUNCTION_RETURN_VALUE -99

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
                 std::shared_ptr<faabric::BatchExecuteRequest> reqIn);

    // Delete everything copy-related, default everything move-related
    ExecutorTask(const ExecutorTask& other) = delete;

    ExecutorTask& operator=(const ExecutorTask& other) = delete;

    ExecutorTask(ExecutorTask&& other) = default;

    ExecutorTask& operator=(ExecutorTask&& other) = default;

    std::shared_ptr<faabric::BatchExecuteRequest> req;
    int messageIndex = 0;
};

class Executor
{
  public:
    std::string id;

    explicit Executor(faabric::Message& msg);

    // Must be marked virtual to permit proper calling of subclass destructors
    virtual ~Executor();

    std::vector<std::pair<uint32_t, int32_t>> executeThreads(
      std::shared_ptr<faabric::BatchExecuteRequest> req,
      const std::vector<faabric::util::SnapshotMergeRegion>& mergeRegions);

    void executeTasks(std::vector<int> msgIdxs,
                      std::shared_ptr<faabric::BatchExecuteRequest> req);

    virtual void shutdown();

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

    long getMillisSinceLastExec();

    virtual std::span<uint8_t> getMemoryView();

    virtual void restore(const std::string& snapshotKey);

    faabric::Message& getBoundMessage();

    bool isExecuting();

    bool isShutdown() { return _isShutdown; }

  protected:
    virtual void setMemorySize(size_t newSize);

    virtual size_t getMaxMemorySize();

    faabric::Message boundMessage;

    Scheduler& sch;

    faabric::snapshot::SnapshotRegistry& reg;

    std::shared_ptr<faabric::util::DirtyTracker> tracker;

    uint32_t threadPoolSize = 0;

  private:
    // ---- Accounting ----
    std::atomic<bool> claimed = false;
    std::atomic<bool> _isShutdown = false;
    std::atomic<int> batchCounter = 0;
    std::atomic<int> threadBatchCounter = 0;
    faabric::util::TimePoint lastExec;

    // ---- Application threads ----
    std::shared_mutex threadExecutionMutex;
    std::vector<char> dirtyRegions;
    std::vector<std::vector<char>> threadLocalDirtyRegions;
    void deleteMainThreadSnapshot(const faabric::Message& msg);

    // ---- Function execution thread pool ----
    std::mutex threadsMutex;
    std::vector<std::shared_ptr<std::jthread>> threadPoolThreads;
    std::set<int> availablePoolThreads;

    std::vector<faabric::util::Queue<ExecutorTask>> threadTaskQueues;

    void threadPoolThread(std::stop_token st, int threadPoolIdx);
};

/**
 * Background thread that periodically checks if there are migration
 * opportunities for in-flight apps that have opted in to being checked for
 * migrations.
 */
class FunctionMigrationThread : public faabric::util::PeriodicBackgroundThread
{
  public:
    void doWork() override;
};

/**
 * A promise for a future message result with an associated eventfd for use with
 * asio.
 */
class MessageLocalResult final
{
  public:
    std::promise<std::unique_ptr<faabric::Message>> promise;
    int eventFd = -1;

    MessageLocalResult();

    MessageLocalResult(const MessageLocalResult&) = delete;

    inline MessageLocalResult(MessageLocalResult&& other)
    {
        this->operator=(std::move(other));
    }

    MessageLocalResult& operator=(const MessageLocalResult&) = delete;

    inline MessageLocalResult& operator=(MessageLocalResult&& other)
    {
        this->promise = std::move(other.promise);
        this->eventFd = other.eventFd;
        other.eventFd = -1;
        return *this;
    }

    ~MessageLocalResult();

    void setValue(std::unique_ptr<faabric::Message>&& msg);
};

/**
 * Background thread that periodically checks to see if any executors have
 * become stale (i.e. not handled any requests in a given timeout). If any are
 * found, they are removed.
 */
class SchedulerReaperThread : public faabric::util::PeriodicBackgroundThread
{
  public:
    void doWork() override;
};

class Scheduler
{
  public:
    Scheduler();

    ~Scheduler();

    faabric::util::SchedulingDecision makeSchedulingDecision(
      std::shared_ptr<faabric::BatchExecuteRequest> req,
      faabric::util::SchedulingTopologyHint topologyHint =
        faabric::util::SchedulingTopologyHint::NONE);

    void callFunction(faabric::Message& msg, bool forceLocal = false);

    faabric::util::SchedulingDecision callFunctions(
      std::shared_ptr<faabric::BatchExecuteRequest> req);

    faabric::util::SchedulingDecision callFunctions(
      std::shared_ptr<faabric::BatchExecuteRequest> req,
      faabric::util::SchedulingDecision& hint);

    void reset();

    void resetThreadLocalCache();

    void shutdown();

    bool isShutdown() { return _isShutdown; }

    void broadcastSnapshotDelete(const faabric::Message& msg,
                                 const std::string& snapshotKey);

    int reapStaleExecutors();

    long getFunctionExecutorCount(const faabric::Message& msg);

    int getFunctionRegisteredHostCount(const faabric::Message& msg);

    const std::set<std::string>& getFunctionRegisteredHosts(
      const std::string& user,
      const std::string& function,
      bool acquireLock = true);

    void broadcastFlush();

    void flushLocally();

    void setFunctionResult(faabric::Message& msg);

    faabric::Message getFunctionResult(unsigned int messageId, int timeout);

    void getFunctionResultAsync(unsigned int messageId,
                                int timeoutMs,
                                asio::io_context& ioc,
                                asio::any_io_executor& executor,
                                std::function<void(faabric::Message&)> handler);

    void setThreadResult(const faabric::Message& msg,
                         int32_t returnValue,
                         const std::string& key,
                         const std::vector<faabric::util::SnapshotDiff>& diffs);

    void setThreadResultLocally(uint32_t msgId, int32_t returnValue);

    /**
     * Caches a message along with the thread result, to allow the thread result
     * to refer to data held in that message (i.e. snapshot diffs). The message
     * will be destroyed once the thread result is consumed.
     */
    void setThreadResultLocally(uint32_t msgId,
                                int32_t returnValue,
                                faabric::transport::Message& message);

    std::vector<std::pair<uint32_t, int32_t>> awaitThreadResults(
      std::shared_ptr<faabric::BatchExecuteRequest> req);

    int32_t awaitThreadResult(uint32_t messageId);

    void registerThread(uint32_t msgId);

    void deregisterThreads(std::shared_ptr<faabric::BatchExecuteRequest> req);

    void deregisterThread(uint32_t msgId);

    std::vector<uint32_t> getRegisteredThreads();

    size_t getCachedMessageCount();

    void vacateSlot();

    std::string getThisHost();

    std::set<std::string> getAvailableHosts();

    void addHostToGlobalSet();

    void addHostToGlobalSet(const std::string& host);

    void removeHostFromGlobalSet(const std::string& host);

    void removeRegisteredHost(const std::string& host,
                              const std::string& user,
                              const std::string& function);

    void addRegisteredHost(const std::string& host,
                           const std::string& user,
                           const std::string& function);

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
    std::shared_ptr<faabric::scheduler::FunctionCallClient>
    getFunctionCallClient(const std::string& otherHost);

    std::shared_ptr<faabric::snapshot::SnapshotClient> getSnapshotClient(
      const std::string& otherHost);

  private:
    std::string thisHost;

    faabric::util::SystemConfig& conf;

    std::shared_mutex mx;

    std::atomic<bool> _isShutdown = false;

    // ---- Executors ----
    std::unordered_map<std::string, std::vector<std::shared_ptr<Executor>>>
      executors;

    // ---- Threads ----
    faabric::snapshot::SnapshotRegistry& reg;

    std::unordered_map<uint32_t, std::promise<int32_t>> threadResults;
    std::unordered_map<uint32_t, faabric::transport::Message>
      threadResultMessages;

    std::unordered_map<uint32_t, std::shared_ptr<MessageLocalResult>>
      localResults;

    std::unordered_map<std::string, std::set<std::string>> pushedSnapshotsMap;

    std::mutex localResultsMutex;

    // ---- Host resources and hosts ----
    faabric::HostResources thisHostResources;
    std::atomic<int32_t> thisHostUsedSlots = 0;

    void updateHostResources();

    faabric::HostResources getHostResources(const std::string& host);

    // ---- Actual scheduling ----
    SchedulerReaperThread reaperThread;

    std::set<std::string> availableHostsCache;

    std::unordered_map<std::string, std::set<std::string>> registeredHosts;

    faabric::util::SchedulingDecision doSchedulingDecision(
      std::shared_ptr<faabric::BatchExecuteRequest> req,
      faabric::util::SchedulingTopologyHint topologyHint);

    faabric::util::SchedulingDecision doCallFunctions(
      std::shared_ptr<faabric::BatchExecuteRequest> req,
      faabric::util::SchedulingDecision& decision,
      faabric::util::FullLock& lock,
      faabric::util::SchedulingTopologyHint topologyHint);

    std::shared_ptr<Executor> claimExecutor(
      faabric::Message& msg,
      faabric::util::FullLock& schedulerLock);

    std::vector<std::string> getUnregisteredHosts(const std::string& user,
                                                  const std::string& function,
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
