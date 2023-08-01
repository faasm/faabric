#include <catch2/catch.hpp>

#include "fixtures.h"

#include <faabric/batch-scheduler/BatchScheduler.h>
#include <faabric/batch-scheduler/BinPackScheduler.h>

using namespace faabric::batch_scheduler;

namespace tests {

class BinPackSchedulerTestFixture : public BatchSchedulerFixture
{
  public:
    BinPackSchedulerTestFixture()
    {
        conf.batchSchedulerMode = "bin-pack";
        batchScheduler = getBatchScheduler();
    }
};

TEST_CASE_METHOD(BinPackSchedulerTestFixture,
                 "Test scheduling of new requests",
                 "[batch-scheduler]")
{
    // To mock new requests (i.e. DecisionType::NEW), we always set the
    // InFlightReqs map to an empty  map
    BatchSchedulerConfig config = {
        .hostMap = {},
        .inFlightReqs = {},
        .expectedDecision = faabric::util::SchedulingDecision(appId, groupId),
    };

    SECTION("BinPack scheduler gives up if not enough slots are available")
    {
        config.hostMap = buildHostMap({ "foo", "bar" }, { 1, 1 }, { 0, 0 });
        ber = faabric::util::batchExecFactory("bat", "man", 6);
        config.expectedDecision = NOT_ENOUGH_SLOTS_DECISION;
    }

    SECTION("Scheduling fits in one host")
    {
        config.hostMap = buildHostMap({ "foo", "bar" }, { 4, 3 }, { 0, 0 });
        ber = faabric::util::batchExecFactory("bat", "man", 3);
        config.expectedDecision =
          buildExpectedDecision(ber, { "foo", "foo", "foo" });
    }

    SECTION("Scheduling is exactly one host")
    {
        config.hostMap = buildHostMap({ "foo", "bar" }, { 4, 3 }, { 0, 0 });
        ber = faabric::util::batchExecFactory("bat", "man", 4);
        config.expectedDecision =
          buildExpectedDecision(ber, { "foo", "foo", "foo", "foo" });
    }

    // The bin-pack scheduler will pick hosts with larger empty slots first
    SECTION("Scheduling spans two hosts")
    {
        config.hostMap = buildHostMap({ "foo", "bar" }, { 4, 3 }, { 0, 0 });
        ber = faabric::util::batchExecFactory("bat", "man", 6);
        config.expectedDecision = buildExpectedDecision(
          ber, { "foo", "foo", "foo", "foo", "bar", "bar" });
    }

    SECTION("Scheduling spans exactly two hosts")
    {
        config.hostMap = buildHostMap({ "foo", "bar" }, { 4, 3 }, { 0, 0 });
        ber = faabric::util::batchExecFactory("bat", "man", 7);
        config.expectedDecision = buildExpectedDecision(
          ber, { "foo", "foo", "foo", "foo", "bar", "bar", "bar" });
    }

    // In particular, it will prioritise hosts with overall less capacity if
    // they have more free resources
    SECTION("Scheduling spans two hosts")
    {
        config.hostMap = buildHostMap({ "foo", "bar" }, { 3, 4 }, { 0, 2 });
        ber = faabric::util::batchExecFactory("bat", "man", 4);
        config.expectedDecision =
          buildExpectedDecision(ber, { "foo", "foo", "foo", "bar" });
    }

    // In case of a tie in free resources, the BinPack scheduler will pick
    // hosts with larger overall capacity first
    SECTION("Scheduling spans two hosts with same free resources")
    {
        config.hostMap = buildHostMap({ "foo", "bar" }, { 4, 3 }, { 1, 0 });
        ber = faabric::util::batchExecFactory("bat", "man", 6);
        config.expectedDecision = buildExpectedDecision(
          ber, { "foo", "foo", "foo", "bar", "bar", "bar" });
    }

    // If there's still a tie, the BinPack scheduler will solve the tie by
    // sorting the hosts alphabetically (from larger to smaller)
    SECTION("Scheduling spans two hosts with same free resources and size")
    {
        config.hostMap = buildHostMap({ "foo", "bar" }, { 3, 3 }, { 0, 0 });
        ber = faabric::util::batchExecFactory("bat", "man", 6);
        config.expectedDecision = buildExpectedDecision(
          ber, { "foo", "foo", "foo", "bar", "bar", "bar" });
    }

    SECTION("Scheduling spans an arbitrarily large number of hosts")
    {
        config.hostMap = buildHostMap({ "foo", "bar", "baz", "bip", "bup" },
                                      { 4, 6, 2, 3, 1 },
                                      { 0, 2, 2, 2, 0 });
        ber = faabric::util::batchExecFactory("bat", "man", 10);
        config.expectedDecision = buildExpectedDecision(ber,
                                                        { "bar",
                                                          "bar",
                                                          "bar",
                                                          "bar",
                                                          "foo",
                                                          "foo",
                                                          "foo",
                                                          "foo",
                                                          "bip",
                                                          "bup" });
    }

    actualDecision = *batchScheduler->makeSchedulingDecision(
      config.hostMap, config.inFlightReqs, ber);
    compareSchedulingDecisions(actualDecision, config.expectedDecision);
}
}
