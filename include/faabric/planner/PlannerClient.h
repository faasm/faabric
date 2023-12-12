#pragma once

#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/planner/planner.pb.h>
#include <faabric/snapshot/SnapshotClient.h>
#include <faabric/transport/MessageEndpointClient.h>
#include <faabric/util/PeriodicBackgroundThread.h>

#include <future>
#include <shared_mutex>

namespace faabric::planner {

typedef std::promise<std::shared_ptr<faabric::Message>> MessageResultPromise;
typedef std::shared_ptr<MessageResultPromise> MessageResultPromisePtr;

/* The planner's implementation of group membership requires clients to send
 * keep-alive messages. Once started, this background thread will send these
 * messages
 */
class KeepAliveThread : public faabric::util::PeriodicBackgroundThread
{
  public:
    void doWork() override;

    void setRequest(std::shared_ptr<RegisterHostRequest> thisHostReqIn);

    // Register request that we can re-use at every check period
    std::shared_ptr<RegisterHostRequest> thisHostReq = nullptr;

  private:
    std::shared_mutex keepAliveThreadMx;
};

/*
 * Local state associated with the current host, used to store useful state
 * like cached results to unnecessary interactions with the planner server.
 */
struct PlannerCache
{
    std::unordered_map<uint32_t, MessageResultPromisePtr> plannerResults;
    // Keeps track of the snapshots that have been pushed to the planner
    std::set<std::string> pushedSnapshots;
};

/*
 * The planner client is used to communicate with the planner over the network.
 * To minimise the number of open connections, we have one static instance
 * of the client per-host. This means that the planner client is reentrant.
 */
class PlannerClient final : public faabric::transport::MessageEndpointClient
{
  public:
    PlannerClient();

    PlannerClient(const std::string& plannerIp);

    void ping();

    void clearCache();

    // ------
    // Host membership calls
    // ------

    std::vector<Host> getAvailableHosts();

    // Registering a host returns the keep-alive timeout for heartbeats
    int registerHost(std::shared_ptr<RegisterHostRequest> req);

    void removeHost(std::shared_ptr<RemoveHostRequest> req);

    // ------
    // Scheduling calls
    // ------

    void setMessageResult(std::shared_ptr<faabric::Message> msg);

    void setMessageResultLocally(std::shared_ptr<faabric::Message> msg);

    faabric::Message getMessageResult(int appId, int msgId, int timeoutMs);

    faabric::Message getMessageResult(const faabric::Message& msg,
                                      int timeoutMs);

    std::shared_ptr<faabric::BatchExecuteRequestStatus> getBatchResults(
      std::shared_ptr<faabric::BatchExecuteRequest> req);

    faabric::batch_scheduler::SchedulingDecision callFunctions(
      std::shared_ptr<faabric::BatchExecuteRequest> req);

    faabric::batch_scheduler::SchedulingDecision getSchedulingDecision(
      std::shared_ptr<faabric::BatchExecuteRequest> req);

    int getNumMigrations();

    void preloadSchedulingDecision(
      std::shared_ptr<faabric::batch_scheduler::SchedulingDecision> preloadDec);

  private:
    std::mutex plannerCacheMx;
    PlannerCache cache;

    // Snapshot client for the planner snapshot server
    std::shared_ptr<faabric::snapshot::SnapshotClient> snapshotClient;

    faabric::Message doGetMessageResult(
      std::shared_ptr<faabric::Message> msgPtr,
      int timeoutMs);

    // This method actually gets the message result from the planner (i.e.
    // sends a request to the planner server)
    std::shared_ptr<faabric::Message> getMessageResultFromPlanner(
      std::shared_ptr<faabric::Message> msg);
};

// -----------------------------------
// Static setter/getters
// -----------------------------------

PlannerClient& getPlannerClient();
}
