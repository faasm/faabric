#include <catch2/catch.hpp>

#include "fixtures.h"

#include <faabric/batch-scheduler/BatchScheduler.h>
#include <faabric/batch-scheduler/BinPackScheduler.h>

using namespace faabric::batch_scheduler;

namespace tests {
TEST_CASE_METHOD(ConfFixture,
                 "Test getting the batch scheduler",
                 "[batch-scheduler]")
{
    resetBatchScheduler();

    SECTION("Bin-pack mode") { conf.batchSchedulerMode = "bin-pack"; }

    // Do it twice so that we check the cold and hot path
    std::shared_ptr<BatchScheduler> batchScheduler = nullptr;
    for (int i = 0; i < 2; i++) {
        batchScheduler = getBatchScheduler();

        REQUIRE(batchScheduler != nullptr);

        if (conf.batchSchedulerMode == "bin-pack") {
            REQUIRE(dynamic_cast<BinPackScheduler*>(batchScheduler.get()) !=
                    nullptr);
        }
    }
}

TEST_CASE_METHOD(ConfFixture,
                 "Test getting a non-existant batch scheduler",
                 "[batch-scheduler]")
{
    resetBatchScheduler();

    conf.batchSchedulerMode = "foo-bar";

    REQUIRE_THROWS(getBatchScheduler());
}

TEST_CASE_METHOD(ConfFixture,
                 "Test getting the decision type",
                 "[batch-scheduler]")
{
    resetBatchScheduler();
    conf.batchSchedulerMode = "bin-pack";
    auto batchScheduler = getBatchScheduler();

    InFlightReqs inFlightReqs = {};
    auto ber = faabric::util::batchExecFactory("foo", "bar", 2);

    DecisionType expectedDecisionType = DecisionType::NO_DECISION_TYPE;

    SECTION("New scheduling decision")
    {
        expectedDecisionType = DecisionType::NEW;
    }

    SECTION("Dist-change decision")
    {
        auto decisionPtr =
          std::make_shared<faabric::util::SchedulingDecision>(ber->appid(), 0);
        inFlightReqs[ber->appid()] = std::make_pair(ber, decisionPtr);
        expectedDecisionType = DecisionType::DIST_CHANGE;
    }

    SECTION("Scale-change decision")
    {
        auto decisionPtr =
          std::make_shared<faabric::util::SchedulingDecision>(ber->appid(), 0);
        auto newBer = faabric::util::batchExecFactory("foo", "bar", 1);
        newBer->set_appid(ber->appid());
        inFlightReqs[newBer->appid()] = std::make_pair(newBer, decisionPtr);
        expectedDecisionType = DecisionType::SCALE_CHANGE;
    }

    REQUIRE(batchScheduler->getDecisionType(inFlightReqs, ber) ==
            expectedDecisionType);
}
}
