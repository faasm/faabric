#pragma once

#include <faabric/planner/PlannerClient.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/transport/PointToPointBroker.h>
#include <faabric/util/PeriodicBackgroundThread.h>
#include <faabric/util/clock.h>
#include <faabric/util/queue.h>
#include <faabric/util/snapshot.h>

#include <shared_mutex>

namespace faabric::scheduler {

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

class ChainedCallException : public faabric::util::FaabricException
{
  public:
    explicit ChainedCallException(std::string message)
      : FaabricException(std::move(message))
    {}
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

    void addChainedMessage(const faabric::Message& msg);

    const faabric::Message& getChainedMessage(int messageId);

    std::set<unsigned int> getChainedMessageIds();

  protected:
    virtual void setMemorySize(size_t newSize);

    virtual size_t getMaxMemorySize();

    faabric::Message boundMessage;

    Scheduler& sch;

    faabric::snapshot::SnapshotRegistry& reg;

    std::shared_ptr<faabric::util::DirtyTracker> tracker;

    uint32_t threadPoolSize = 0;

    std::map<int, std::shared_ptr<faabric::Message>> chainedMessages;

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

    void executeBatch(std::shared_ptr<faabric::BatchExecuteRequest> req);

    void reset();

    void resetThreadLocalCache();

    void shutdown();

    bool isShutdown() { return _isShutdown; }

    void broadcastSnapshotDelete(const faabric::Message& msg,
                                 const std::string& snapshotKey);

    int reapStaleExecutors();

    long getFunctionExecutorCount(const faabric::Message& msg);

    void flushLocally();

    // ----------------------------------
    // Message results
    // ----------------------------------

    void setFunctionResult(faabric::Message& msg);

    void setThreadResult(faabric::Message& msg,
                         int32_t returnValue,
                         const std::string& key,
                         const std::vector<faabric::util::SnapshotDiff>& diffs);

    /**
     * Caches a message along with the thread result, to allow the thread result
     * to refer to data held in that message (i.e. snapshot diffs). The message
     * will be destroyed once the thread result is consumed.
     */
    void setThreadResultLocally(uint32_t appId,
                                uint32_t msgId,
                                int32_t returnValue,
                                faabric::transport::Message& message);

    std::vector<std::pair<uint32_t, int32_t>> awaitThreadResults(
      std::shared_ptr<faabric::BatchExecuteRequest> req);

    size_t getCachedMessageCount();

    std::string getThisHost();

    void addHostToGlobalSet();

    void addHostToGlobalSet(
      const std::string& host,
      std::shared_ptr<faabric::HostResources> overwriteResources = nullptr);

    void removeHostFromGlobalSet(const std::string& host);

    void setThisHostResources(faabric::HostResources& res);

    // ----------------------------------
    // Testing
    // ----------------------------------
    std::vector<faabric::Message> getRecordedMessages();

    void clearRecordedMessages();

    // ----------------------------------
    // Function Migration
    // ----------------------------------
    std::shared_ptr<faabric::PendingMigration> checkForMigrationOpportunities(
      faabric::Message& msg,
      int overwriteNewGroupId = 0);

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

    std::unordered_map<uint32_t, faabric::transport::Message>
      threadResultMessages;

    // ---- Planner----
    faabric::planner::KeepAliveThread keepAliveThread;

    // ---- Actual scheduling ----
    SchedulerReaperThread reaperThread;

    std::shared_ptr<Executor> claimExecutor(
      faabric::Message& msg,
      faabric::util::FullLock& schedulerLock);

    // ---- Accounting and debugging ----
    std::vector<faabric::Message> recordedMessages;

    // ---- Point-to-point ----
    faabric::transport::PointToPointBroker& broker;
};

}
