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
                 "Test scheduling of new requests with BinPack",
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

TEST_CASE_METHOD(BinPackSchedulerTestFixture,
                 "Test scheduling of scale-change requests with BinPack",
                 "[batch-scheduler]")
{
    // To mock a scale-change request (i.e. DecisionType::SCALE_CHANGE), we
    // need to have one in-flight request in the map with the same app id
    // (and not of type MIGRATION)
    BatchSchedulerConfig config = {
        .hostMap = {},
        .inFlightReqs = {},
        .expectedDecision = faabric::util::SchedulingDecision(appId, groupId),
    };

    SECTION("BinPack scheduler gives up if not enough slots are available")
    {
        config.hostMap = buildHostMap({ "foo", "bar" }, { 2, 1 }, { 1, 0 });
        ber = faabric::util::batchExecFactory("bat", "man", 6);
        config.inFlightReqs = buildInFlightReqs(ber, 1, { "foo" });
        config.expectedDecision = NOT_ENOUGH_SLOTS_DECISION;
    }

    // When scheduling a SCALE_CHANGE request, we always try to colocate as
    // much as possible
    SECTION("Scheduling fits in one host")
    {
        config.hostMap = buildHostMap({ "foo", "bar" }, { 4, 3 }, { 1, 0 });
        ber = faabric::util::batchExecFactory("bat", "man", 3);
        config.inFlightReqs = buildInFlightReqs(ber, 1, { "foo" });
        config.expectedDecision =
          buildExpectedDecision(ber, { "foo", "foo", "foo" });
    }

    // We prefer hosts with less capacity if they are already running requests
    // for the same app
    SECTION("Scheduling fits in one host and prefers known hosts")
    {
        config.hostMap = buildHostMap({ "foo", "bar" }, { 5, 4 }, { 0, 1 });
        ber = faabric::util::batchExecFactory("bat", "man", 3);
        config.inFlightReqs = buildInFlightReqs(ber, 1, { "bar" });
        config.expectedDecision =
          buildExpectedDecision(ber, { "bar", "bar", "bar" });
    }

    // Like with `NEW` requests, we can also spill to other hosts
    SECTION("Scheduling spans more than one host")
    {
        config.hostMap = buildHostMap({ "foo", "bar" }, { 4, 3 }, { 0, 1 });
        ber = faabric::util::batchExecFactory("bat", "man", 4);
        config.inFlightReqs = buildInFlightReqs(ber, 1, { "bar" });
        config.expectedDecision =
          buildExpectedDecision(ber, { "bar", "bar", "foo", "foo" });
    }

    // If two hosts are already executing the app, we pick the one that is
    // running the largest number of messages
    SECTION("Scheduler prefers hosts with more running messages")
    {
        config.hostMap = buildHostMap({ "foo", "bar" }, { 4, 3 }, { 1, 2 });
        ber = faabric::util::batchExecFactory("bat", "man", 1);
        config.inFlightReqs =
          buildInFlightReqs(ber, 3, { "bar", "bar", "foo" });
        config.expectedDecision = buildExpectedDecision(ber, { "bar" });
    }

    // Again, when picking a new host to spill to, we priorities hosts that
    // are already running requests for this app
    SECTION("Scheduling always picks known hosts first")
    {
        config.hostMap = buildHostMap(
          {
            "foo",
            "bar",
            "baz",
          },
          { 4, 3, 2 },
          { 0, 1, 1 });
        ber = faabric::util::batchExecFactory("bat", "man", 5);
        config.inFlightReqs = buildInFlightReqs(ber, 2, { "bar", "baz" });
        config.expectedDecision =
          buildExpectedDecision(ber, { "bar", "bar", "baz", "foo", "foo" });
    }

    // Sometimes the preferred hosts just don't have slots. They will be sorted
    // first but the scheduler will skip them when bin-packing
    SECTION("Scheduler ignores preferred but full hosts")
    {
        config.hostMap = buildHostMap(
          {
            "foo",
            "bar",
            "baz",
          },
          { 4, 2, 2 },
          { 0, 2, 1 });
        ber = faabric::util::batchExecFactory("bat", "man", 3);
        config.inFlightReqs =
          buildInFlightReqs(ber, 3, { "bar", "bar", "baz" });
        config.expectedDecision =
          buildExpectedDecision(ber, { "baz", "foo", "foo" });
    }

    // In case of a tie of the number of runing messages, we revert to `NEW`-
    // like tie breaking
    SECTION("In case of a tie of preferred hosts, fall-back to known "
            "tie-breaks (free slots)")
    {
        config.hostMap = buildHostMap(
          {
            "foo",
            "bar",
            "baz",
          },
          { 4, 3, 2 },
          { 0, 1, 1 });
        ber = faabric::util::batchExecFactory("bat", "man", 3);
        config.inFlightReqs = buildInFlightReqs(ber, 2, { "bar", "baz" });
        config.expectedDecision =
          buildExpectedDecision(ber, { "bar", "bar", "baz" });
    }

    SECTION("In case of a tie of preferred hosts, fall-back to known "
            "tie-breaks (size)")
    {
        config.hostMap = buildHostMap(
          {
            "foo",
            "bar",
            "baz",
          },
          { 4, 3, 2 },
          { 0, 2, 1 });
        ber = faabric::util::batchExecFactory("bat", "man", 3);
        config.inFlightReqs = buildInFlightReqs(ber, 2, { "bar", "baz" });
        config.expectedDecision =
          buildExpectedDecision(ber, { "bar", "baz", "foo" });
    }

    SECTION("In case of a tie of preferred hosts, fall-back to known "
            "tie-breaks (alphabetical)")
    {
        config.hostMap = buildHostMap(
          {
            "foo",
            "bar",
            "baz",
          },
          { 4, 2, 2 },
          { 0, 1, 1 });
        ber = faabric::util::batchExecFactory("bat", "man", 3);
        config.inFlightReqs = buildInFlightReqs(ber, 2, { "bar", "baz" });
        config.expectedDecision =
          buildExpectedDecision(ber, { "baz", "bar", "foo" });
    }

    actualDecision = *batchScheduler->makeSchedulingDecision(
      config.hostMap, config.inFlightReqs, ber);
    compareSchedulingDecisions(actualDecision, config.expectedDecision);
}

TEST_CASE_METHOD(BinPackSchedulerTestFixture,
                 "Test scheduling of dist-change requests with BinPack",
                 "[batch-scheduler]")
{
    // To mock a dist-change request (i.e. DecisionType::DIST_CHANGE), we
    // need to have one in-flight request in the map with the same app id, the
    // same size (and of type MIGRATION)
    BatchSchedulerConfig config = {
        .hostMap = {},
        .inFlightReqs = {},
        .expectedDecision = faabric::util::SchedulingDecision(appId, groupId),
    };

    // The configs in this test must be read as follows:
    // - the host map's used slots contains the current distribution for the app
    // - the host map's slots contain the total slots, there is a migration
    //   opportunity if we can improve the current distribution
    // - we repeat the distribtution when building the in-flight requests (but
    //   also the host names)

    // Given a migration (defined by the number of cross-VM links, or
    // equivalently the host-to-message histogram), the BinPack scheduler will
    // try to minimise the number of messages to actually be migrated
    SECTION("BinPack will minimise the number of messages to migrate")
    {
        config.hostMap =
          buildHostMap({ "foo", "bar", "baz" }, { 5, 4, 2 }, { 3, 4, 2 });
        ber = faabric::util::batchExecFactory("bat", "man", 9);
        ber->set_type(BatchExecuteRequest_BatchExecuteType_MIGRATION);
        config.inFlightReqs = buildInFlightReqs(
          ber,
          9,
          { "foo", "foo", "foo", "bar", "bar", "bar", "bar", "baz", "baz" });
        config.expectedDecision = buildExpectedDecision(
          ber,
          { "foo", "foo", "foo", "bar", "bar", "bar", "bar", "foo", "foo" });
    }

    actualDecision = *batchScheduler->makeSchedulingDecision(
      config.hostMap, config.inFlightReqs, ber);
    compareSchedulingDecisions(actualDecision, config.expectedDecision);
}
}
