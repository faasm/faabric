#pragma once

#include <faabric/uitl/batch.h>
#include <faabric/util/scheduling.h>

#include <string>

namespace faabric::planner::scheduler {

/*
 * Interface class for different scheduler implementations to be used with the
 * planner.
 */
class Scheduler {
  public:
    Scheduler(const std::string& modeIn)
      : mode(modeIn)
    {}

    virtual std::shared_ptr<faabric::util::SchedulingDecision>
    makeSchedulingDecision(std::shared_ptr<faabric::BatchExecuteRequest> req,
                           faabric::util::SchedulingTopologyHint topologyHint =
                            faabric::util::SchedulingTopologyHint::NONE) = 0;


  protected:
    // TODO: should we use an enum instead?
    std::string mode;
};

class BinPackScheduler final : public Scheduler
{
  public:
    BinPackScheduler(const std::string& modeIn);

    std::shared_ptr<faabric::util::SchedulingDecision>
    makeSchedulingDecision(std::shared_ptr<faabric::BatchExecuteRequest> req,
                           faabric::util::SchedulingTopologyHint topologyHint =
                            faabric::util::SchedulingTopologyHint::NONE) override;
};
}
