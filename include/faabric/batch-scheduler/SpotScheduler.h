#pragma once

#include <faabric/batch-scheduler/BatchScheduler.h>
#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/util/batch.h>

namespace faabric::batch_scheduler {

// This batch scheduler behaves in the same way than BinPack for NEW and
// SCALE_CHANGE requests, but for DIST_CHANGE it considers if any of the
// hosts in the Host Map have been tainted with the eviction mark. In which
// case it first tries to migrate them to other running hosts and, if not
// enough hosts are available, freezes the messages.
class SpotScheduler final : public BatchScheduler
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

    std::vector<Host> getSortedHosts(
      HostMap& hostMap,
      const InFlightReqs& inFlightReqs,
      std::shared_ptr<faabric::BatchExecuteRequest> req,
      const DecisionType& decisionType) override;
};
}
