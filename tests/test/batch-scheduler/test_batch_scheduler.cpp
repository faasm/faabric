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
}
