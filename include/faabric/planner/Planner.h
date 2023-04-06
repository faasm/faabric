#pragma once

#include <faabric/planner/PlannerState.h>
#include <faabric/planner/planner.pb.h>

#include <shared_mutex>

namespace faabric::planner {
enum FlushType
{
    NoFlushType = 0,
    Hosts = 1,
};

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
    // Host membership management
    // ----------

    std::vector<std::shared_ptr<Host>> getAvailableHosts();

    bool registerHost(const Host& hostIn, int* hostId);

    // Best effort host removal. Don't fail if we can't
    void removeHost(const Host& hostIn);

  private:
    // There's a singletone instance of the planner running, but it must allow
    // concurrent requests
    std::shared_mutex plannerMx;

    PlannerState state;
    PlannerConfig config;

    void flushHosts();

    // Check if a host's registration timestamp has expired
    bool isHostExpired(std::shared_ptr<Host> host, long epochTimeMs = 0);
};

Planner& getPlanner();
}
