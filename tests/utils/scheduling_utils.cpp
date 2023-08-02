#include <catch2/catch.hpp>

#include "faabric_utils.h"

namespace tests {

void checkSchedulingDecisionEquality(
  const faabric::batch_scheduler::SchedulingDecision& decisionA,
  const faabric::batch_scheduler::SchedulingDecision& decisionB)
{
    REQUIRE(decisionA.appId == decisionB.appId);
    REQUIRE(decisionA.nFunctions == decisionB.nFunctions);
    REQUIRE(decisionA.messageIds == decisionB.messageIds);
    REQUIRE(decisionA.hosts == decisionB.hosts);
    REQUIRE(decisionA.appIdxs == decisionB.appIdxs);
    REQUIRE(decisionA.returnHost == decisionB.returnHost);
}
}
