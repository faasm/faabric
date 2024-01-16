#pragma once

#include <faabric/executor/Executor.h>
#include <faabric/planner/PlannerClient.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/transport/PointToPointBroker.h>
#include <faabric/util/PeriodicBackgroundThread.h>
#include <faabric/util/clock.h>
#include <faabric/util/queue.h>
#include <faabric/util/snapshot.h>

#include <shared_mutex>

#define DEFAULT_THREAD_RESULT_TIMEOUT_MS 1000

namespace faabric::scheduler {

class Scheduler;

Scheduler& getScheduler();

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

    // ----------------------------------
    // Message results
    // ----------------------------------

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
      std::shared_ptr<faabric::BatchExecuteRequest> req,
      int timeoutMs = DEFAULT_THREAD_RESULT_TIMEOUT_MS);

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
    std::unordered_map<
      std::string,
      std::vector<std::shared_ptr<faabric::executor::Executor>>>
      executors;

    // ---- Threads ----
    faabric::snapshot::SnapshotRegistry& reg;

    std::unordered_map<uint32_t, faabric::transport::Message>
      threadResultMessages;

    // ---- Planner----
    faabric::planner::KeepAliveThread keepAliveThread;

    // ---- Actual scheduling ----
    SchedulerReaperThread reaperThread;

    std::shared_ptr<faabric::executor::Executor> claimExecutor(
      faabric::Message& msg,
      faabric::util::FullLock& schedulerLock);

    // ---- Accounting and debugging ----
    std::vector<faabric::Message> recordedMessages;

    // ---- Point-to-point ----
    faabric::transport::PointToPointBroker& broker;
};

}
