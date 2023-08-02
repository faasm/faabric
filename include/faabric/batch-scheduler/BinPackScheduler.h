#pragma once

#include <faabric/batch-scheduler/BatchScheduler.h>
#include <faabric/util/batch.h>
#include <faabric/util/scheduling.h>
#include <string>

namespace faabric::batch_scheduler {

class BinPackScheduler final : public BatchScheduler
{
  public:
    BinPackScheduler(const std::string& modeIn)
      : BatchScheduler(modeIn){};

    std::shared_ptr<faabric::util::SchedulingDecision> makeSchedulingDecision(
      const HostMap& hostMap,
      const InFlightReqs& inFlightReqs,
      std::shared_ptr<faabric::BatchExecuteRequest> req) override;

  private:
    bool isFirstDecisionBetter(
      std::shared_ptr<faabric::util::SchedulingDecision> decisionA,
      std::shared_ptr<faabric::util::SchedulingDecision> decisionB) override;

    std::vector<Host> getSortedHosts(
      const HostMap& hostMap,
      const InFlightReqs& inFlightReqs,
      std::shared_ptr<faabric::BatchExecuteRequest> req,
      const DecisionType& decisionType) override;
};
}
