#pragma once

#include <faabric/planner/PlannerState.h>
#include <faabric/planner/planner.pb.h>
#include <faabric/util/asio.h>
#include <faabric/util/scheduling.h>

#include <shared_mutex>

namespace faabric::planner {
enum FlushType
{
    NoFlushType = 0,
    Hosts = 1,
    Executors = 2,
};

/* The planner is a standalone component that has a global view of the state
 * of a distributed faabric deployment.
 */
class Planner
{
  public:
    Planner();

    // ----------
    // Planner config
    // ----------

    PlannerConfig getConfig();

    void printConfig() const;

    void setTestsConfig(PlannerTestsConfig& testsConfigIn, bool reset = false);

    // ----------
    // Util
    // ----------

    bool reset();

    bool flush(faabric::planner::FlushType flushType);

    // ----------
    // Host membership public API
    // ----------

    std::vector<std::shared_ptr<Host>> getAvailableHosts();

    bool registerHost(const Host& hostIn, bool overwrite);

    // Best effort host removal. Don't fail if we can't
    void removeHost(const Host& hostIn);

    // ----------
    // Request scheduling public API
    // ----------

    // Given a batch execute request, make a scheduling decision informed by
    // the topology hint
    std::shared_ptr<faabric::util::SchedulingDecision> makeSchedulingDecision(
      std::shared_ptr<faabric::BatchExecuteRequest> req,
      faabric::util::SchedulingTopologyHint topologyHint =
        faabric::util::SchedulingTopologyHint::NONE);

    void dispatchSchedulingDecision(
      std::shared_ptr<faabric::BatchExecuteRequest> req,
      std::shared_ptr<faabric::util::SchedulingDecision> decision);

    // Getters for planner's scheduling state

    std::vector<std::shared_ptr<faabric::BatchExecuteRequest>> getInFlightBatches();

    std::shared_ptr<faabric::util::SchedulingDecision> getSchedulingDecision(
      std::shared_ptr<faabric::BatchExecuteRequest> req);

    std::shared_ptr<faabric::BatchExecuteRequest> getBatchMessages(
      std::shared_ptr<faabric::BatchExecuteRequest> req);

    // Setters/getters for individual message results

    void setMessageResult(std::shared_ptr<faabric::Message> msg);

    std::shared_ptr<faabric::Message> getMessageResult(
      std::shared_ptr<faabric::Message> msg);

    PlannerTestsConfig testsConfig;
    std::atomic<bool> isTestMode = false;

    // ----------
    // Host membership private API
    // ----------

    std::vector<std::shared_ptr<Host>> doGetAvailableHosts();

    void flushHosts();

    void flushExecutors();

    // Check if a host's registration timestamp has expired
    bool isHostExpired(std::shared_ptr<Host> host, long epochTimeMs = 0);

    // ----------
    // Scheduling private API
    // ----------

    void flushSchedulingState();
};

Planner& getPlanner();
}
