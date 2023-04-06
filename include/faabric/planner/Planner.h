#pragma once

#include <faabric/planner/planner.pb.h>
#include <faabric/planner/PlannerState.h>

#include <shared_mutex>

namespace faabric::planner {
class Planner
{
  public:
    // TODO: initialise config
    Planner();

    PlannerConfig getConfig();

    void printConfig() const;

    bool registerHost(const Host& hostIn, int* hostId);

  private:
    // There's a singletone instance of the planner running, but it must allow
    // concurrent requests
    std::shared_mutex plannerMx;

    PlannerState state;
    PlannerConfig config;
};

Planner& getPlanner();
}
