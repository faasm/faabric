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
    // the topology hint. If the request is already in-flight, WHAT?
    std::shared_ptr<faabric::util::SchedulingDecision> makeSchedulingDecision(
      std::shared_ptr<faabric::BatchExecuteRequest> req,
      faabric::util::SchedulingTopologyHint topologyHint =
        faabric::util::SchedulingTopologyHint::NONE);

    void dispatchSchedulingDecision(
      std::shared_ptr<faabric::BatchExecuteRequest> req,
      std::shared_ptr<faabric::util::SchedulingDecision> decision);

    // Getters for planner's scheduling state

    std::shared_ptr<faabric::util::SchedulingDecision> getSchedulingDecision(
      std::shared_ptr<faabric::BatchExecuteRequest> req);

    std::shared_ptr<faabric::BatchExecuteRequest> getBatchMessages(
      std::shared_ptr<faabric::BatchExecuteRequest> req);

    // Setters/getters for individual message results

    void setMessageResult(std::shared_ptr<faabric::Message> msg);

    std::shared_ptr<faabric::Message> getMessageResult(
      std::shared_ptr<faabric::Message> msg);

    /* TODO: finish me!
    void getMessageResultAsync(
      std::shared_ptr<faabric::Message> msg,
      int timeoutMs,
      asio::io_context& ioc,
      asio::any_io_executor& executor,
      std::function<void(faabric::Message&)> handler);
    */

  private:
    // There's a singleton instance of the planner running, but it must allow
    // concurrent requests
    std::shared_mutex plannerMx;

    PlannerState state;
    PlannerConfig config;
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