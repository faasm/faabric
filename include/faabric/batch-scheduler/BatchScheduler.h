#pragma once

#include <faabric/util/batch.h>
#include <faabric/util/scheduling.h>

#include <string>

namespace faabric::batch_scheduler {

// TODO: move BatchExecuteRequest here

// TODO: move SchedulingDecision here?
typedef std::pair<std::shared_ptr<BatchExecuteRequest>,
                  std::shared_ptr<faabric::util::SchedulingDecision>>
  InFlightPair;

typedef std::map<int32_t, InFlightPair> InFlightReqs;

// TODO: remove duplication with PlannerState
struct HostState
{
    std::string ip;
    int slots;
    int usedSlots;
};
typedef std::map<std::string, std::shared_ptr<HostState>> HostMap;

/*
 * Interface class for different scheduler implementations to be used with the
 * planner.
 */
class BatchScheduler
{
  public:
    BatchScheduler(const std::string& modeIn)
      : mode(modeIn)
    {}

    virtual std::shared_ptr<faabric::util::SchedulingDecision>
    makeSchedulingDecision(
      const HostMap& hostMap,
      const InFlightReqs& inFlightReqs,
      std::shared_ptr<faabric::BatchExecuteRequest> req) = 0;

  protected:
    // TODO: should we use an enum instead?
    std::string mode;
};

std::shared_ptr<BatchScheduler> getBatchScheduler();

void resetBatchScheduler();
}
