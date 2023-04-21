#pragma once

#include <faabric/planner/PlannerState.h>
#include <faabric/planner/planner.pb.h>
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

    // ----------
    // Util
    // ----------

    bool reset();

    bool flush(faabric::planner::FlushType flushType);

    // ----------
    // Host membership public API
    // ----------

    std::vector<std::shared_ptr<Host>> getAvailableHosts();

    bool registerHost(const Host& hostIn);

    // Best effort host removal. Don't fail if we can't
    void removeHost(const Host& hostIn);

    // ----------
    // Request scheduling public API
    // ----------
    // TODO: remove duplication with src/scheduler/Scheduler.cpp, and move
    // away this methods elsewhere

    // Given a batch execute request, make a scheduling decision informed by
    // the topology hint. If the request is already in-flight, WHAT?
    std::shared_ptr<faabric::util::SchedulingDecision> makeSchedulingDecision(
      std::shared_ptr<faabric::BatchExecuteRequest> req,
      faabric::util::SchedulingTopologyHint topologyHint =
        faabric::util::SchedulingTopologyHint::NONE);

    void dispatchSchedulingDecision(
      std::shared_ptr<faabric::BatchExecuteRequest> req,
      std::shared_ptr<faabric::util::SchedulingDecision> decision);

    // Legacy set/get message result methods called from local schedulers
    void setMessageResult(std::shared_ptr<faabric::Message> msg);

    std::shared_ptr<faabric::Message> getMessageResult(
      std::shared_ptr<faabric::Message> msg);

    // bool waitForAppResult(std::shared_ptr<faabric::BatchExecuteRequest> req);

  private:
    // There's a singleton instance of the planner running, but it must allow
    // concurrent requests
    std::shared_mutex plannerMx;

    PlannerState state;
    PlannerConfig config;

    // ----------
    // Host membership private API
    // ----------

    std::vector<std::shared_ptr<Host>> doGetAvailableHosts();

    void flushHosts();

    void flushExecutors();

    // Check if a host's registration timestamp has expired
    bool isHostExpired(std::shared_ptr<Host> host, long epochTimeMs = 0);
};

Planner& getPlanner();
}
