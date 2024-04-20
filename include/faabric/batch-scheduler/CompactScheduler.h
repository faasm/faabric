#pragma once

#include <faabric/batch-scheduler/BatchScheduler.h>
#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/util/batch.h>

namespace faabric::batch_scheduler {

// This batch scheduler behaves in the same way than BinPack for NEW and
// SCALE_CHANGE requests, but for DIST_CHANGE requests it tries to compact
// to the fewest number of VMs.
class CompactScheduler final : public BatchScheduler
{
  public:
    std::shared_ptr<SchedulingDecision> makeSchedulingDecision(
      HostMap& hostMap,
      const InFlightReqs& inFlightReqs,
      std::shared_ptr<faabric::BatchExecuteRequest> req) override;

  private:
    bool isFirstDecisionBetter(
      std::shared_ptr<SchedulingDecision> decisionA,
      std::shared_ptr<SchedulingDecision> decisionB) override;

    bool isFirstDecisionBetter(HostMap& hostMap,
                               std::shared_ptr<SchedulingDecision> decisionA,
                               std::shared_ptr<SchedulingDecision> decisionB);

    std::vector<Host> getSortedHosts(
      HostMap& hostMap,
      const InFlightReqs& inFlightReqs,
      std::shared_ptr<faabric::BatchExecuteRequest> req,
      const DecisionType& decisionType) override;
};
}
