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
        .expectedDecision = SchedulingDecision(appId, groupId),
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
        .expectedDecision = SchedulingDecision(appId, groupId),
    };

    // The configs in this test must be read as follows:
    // - the host map's used slots contains the current distribution for the app
    //   (i.e. the number of used slots matches the number in in-flight reqs)
    // - the host map's slots contain the total slots
    // - the ber contains the NEW messages we are going to add
    // - the expected decision includes the expected scheduling decision for
    //   the new messages

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
        .expectedDecision = SchedulingDecision(appId, groupId),
    };

    // The configs in this test must be read as follows:
    // - the host map's used slots contains the current distribution for the app
    // - the host map's slots contain the total slots, there is a migration
    //   opportunity if we can improve the current distribution
    // - we repeat the distribtution when building the in-flight requests (but
    //   also the host names)

    SECTION("BinPack returns nothing if there's no opportunity to migrate "
            "(single host)")
    {
        config.hostMap = buildHostMap({ "foo" }, { 4 }, { 2 });
        ber = faabric::util::batchExecFactory("bat", "man", 2);
        ber->set_type(BatchExecuteRequest_BatchExecuteType_MIGRATION);
        config.inFlightReqs = buildInFlightReqs(ber, 2, { "foo", "foo" });
        config.expectedDecision = DO_NOT_MIGRATE_DECISION;
    }

    SECTION("BinPack returns nothing if there's no opportunity to migrate "
            "(multiple hosts)")
    {
        config.hostMap = buildHostMap({ "foo", "bar" }, { 4, 2 }, { 4, 1 });
        ber = faabric::util::batchExecFactory("bat", "man", 5);
        ber->set_type(BatchExecuteRequest_BatchExecuteType_MIGRATION);
        config.inFlightReqs =
          buildInFlightReqs(ber, 5, { "foo", "foo", "foo", "foo", "bar" });
        config.expectedDecision = DO_NOT_MIGRATE_DECISION;
    }

    SECTION("BinPack detects opportunities to consolidate to one host")
    {
        config.hostMap = buildHostMap({ "foo", "bar" }, { 4, 2 }, { 2, 2 });
        ber = faabric::util::batchExecFactory("bat", "man", 4);
        ber->set_type(BatchExecuteRequest_BatchExecuteType_MIGRATION);
        config.inFlightReqs =
          buildInFlightReqs(ber, 4, { "foo", "foo", "bar", "bar" });
        config.expectedDecision =
          buildExpectedDecision(ber, { "foo", "foo", "foo", "foo" });
    }

    SECTION(
      "In case of a tie, it uses the same tie-break than NEW (free slots)")
    {
        config.hostMap = buildHostMap({ "foo", "bar" }, { 4, 5 }, { 2, 2 });
        ber = faabric::util::batchExecFactory("bat", "man", 4);
        ber->set_type(BatchExecuteRequest_BatchExecuteType_MIGRATION);
        config.inFlightReqs =
          buildInFlightReqs(ber, 4, { "foo", "foo", "bar", "bar" });
        config.expectedDecision =
          buildExpectedDecision(ber, { "bar", "bar", "bar", "bar" });
    }

    SECTION(
      "In case of a tie, it uses the same tie-break than NEW (total capacity)")
    {
        config.hostMap = buildHostMap({ "foo", "bar" }, { 4, 5 }, { 2, 3 });
        ber = faabric::util::batchExecFactory("bat", "man", 4);
        ber->set_type(BatchExecuteRequest_BatchExecuteType_MIGRATION);
        config.inFlightReqs =
          buildInFlightReqs(ber, 4, { "foo", "foo", "bar", "bar" });
        config.expectedDecision =
          buildExpectedDecision(ber, { "bar", "bar", "bar", "bar" });
    }

    SECTION("In case of a tie, it uses the same tie-break than NEW (larger "
            "alphabetically)")
    {
        config.hostMap = buildHostMap({ "foo", "bar" }, { 4, 4 }, { 2, 2 });
        ber = faabric::util::batchExecFactory("bat", "man", 4);
        ber->set_type(BatchExecuteRequest_BatchExecuteType_MIGRATION);
        config.inFlightReqs =
          buildInFlightReqs(ber, 4, { "foo", "foo", "bar", "bar" });
        config.expectedDecision =
          buildExpectedDecision(ber, { "foo", "foo", "foo", "foo" });
    }

    SECTION("BinPack prefers hosts running more messages")
    {
        config.hostMap = buildHostMap(
          {
            "foo",
            "bar",
            "baz",
          },
          { 3, 2, 1 },
          { 2, 1, 1 });
        ber = faabric::util::batchExecFactory("bat", "man", 4);
        ber->set_type(BatchExecuteRequest_BatchExecuteType_MIGRATION);
        config.inFlightReqs =
          buildInFlightReqs(ber, 4, { "foo", "foo", "bar", "baz" });
        config.expectedDecision =
          buildExpectedDecision(ber, { "foo", "foo", "bar", "foo" });
    }

    SECTION("BinPack prefers hosts running more slots (even if less messags)")
    {
        config.hostMap = buildHostMap(
          {
            "foo",
            "bar",
            "baz",
          },
          { 3, 5, 1 },
          { 2, 1, 1 });
        ber = faabric::util::batchExecFactory("bat", "man", 4);
        ber->set_type(BatchExecuteRequest_BatchExecuteType_MIGRATION);
        config.inFlightReqs =
          buildInFlightReqs(ber, 4, { "foo", "foo", "bar", "baz" });
        config.expectedDecision =
          buildExpectedDecision(ber, { "bar", "bar", "bar", "bar" });
    }

    SECTION("BinPack always prefers consolidating to fewer hosts")
    {
        config.hostMap = buildHostMap(
          {
            "foo",
            "bar",
            "baz",
          },
          { 3, 5, 1 },
          { 1, 1, 1 });
        ber = faabric::util::batchExecFactory("bat", "man", 3);
        ber->set_type(BatchExecuteRequest_BatchExecuteType_MIGRATION);
        config.inFlightReqs =
          buildInFlightReqs(ber, 3, { "foo", "bar", "baz" });
        config.expectedDecision =
          buildExpectedDecision(ber, { "bar", "bar", "bar" });
    }

    SECTION("In case of a tie of hosts, it minimises cross-VM links")
    {
        config.hostMap = buildHostMap(
          {
            "foo",
            "bar",
          },
          { 4, 4 },
          { 3, 3 });
        ber = faabric::util::batchExecFactory("bat", "man", 6);
        ber->set_type(BatchExecuteRequest_BatchExecuteType_MIGRATION);
        config.inFlightReqs = buildInFlightReqs(
          ber, 6, { "foo", "foo", "foo", "bar", "bar", "bar" });
        config.expectedDecision = buildExpectedDecision(
          ber, { "foo", "foo", "foo", "bar", "bar", "foo" });
    }

    // Check we correctly minimise cross-VM links in >2 VM scenarios
    SECTION("It also minimises cross-VM links with more than 2 VMs")
    {
        config.hostMap = buildHostMap(
          {
            "foo",
            "bar",
            "baz",
            "bat",
          },
          { 2, 2, 1, 1 },
          { 1, 1, 1, 1 });
        ber = faabric::util::batchExecFactory("bat", "man", 4);
        ber->set_type(BatchExecuteRequest_BatchExecuteType_MIGRATION);
        config.inFlightReqs =
          buildInFlightReqs(ber, 4, { "foo", "bar", "baz", "bat" });
        config.expectedDecision =
          buildExpectedDecision(ber, { "foo", "bar", "bar", "foo" });
    }

    SECTION("BinPack will migrate to completely different hosts if necessary")
    {
        config.hostMap = buildHostMap(
          {
            "foo",
            "bar",
            "baz",
          },
          { 4, 4, 4 },
          { 4, 4, 0 });
        ber = faabric::util::batchExecFactory("bat", "man", 4);
        ber->set_type(BatchExecuteRequest_BatchExecuteType_MIGRATION);
        config.inFlightReqs =
          buildInFlightReqs(ber, 4, { "foo", "foo", "bar", "bar" });
        config.expectedDecision =
          buildExpectedDecision(ber, { "baz", "baz", "baz", "baz" });
    }

    SECTION(
      "But in case of a tie will prefer minimising the number of migrations")
    {
        config.hostMap = buildHostMap(
          {
            "foo",
            "bar",
            "baz",
          },
          { 4, 4, 4 },
          { 0, 4, 2 });
        ber = faabric::util::batchExecFactory("bat", "man", 4);
        ber->set_type(BatchExecuteRequest_BatchExecuteType_MIGRATION);
        config.inFlightReqs =
          buildInFlightReqs(ber, 4, { "baz", "baz", "bar", "bar" });
        config.expectedDecision =
          buildExpectedDecision(ber, { "baz", "baz", "baz", "baz" });
    }

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

    SECTION("BinPack will minimise the number of messages to migrate (ii)")
    {
        config.hostMap =
          buildHostMap({ "foo", "bar", "baz" }, { 5, 3, 2 }, { 2, 3, 2 });
        ber = faabric::util::batchExecFactory("bat", "man", 7);
        ber->set_type(BatchExecuteRequest_BatchExecuteType_MIGRATION);
        config.inFlightReqs = buildInFlightReqs(
          ber, 7, { "bar", "bar", "bar", "baz", "baz", "foo", "foo" });
        config.expectedDecision = buildExpectedDecision(
          ber, { "bar", "bar", "foo", "foo", "foo", "foo", "foo" });
    }

    SECTION("BinPack will minimise the number of messages to migrate (iii)")
    {
        config.hostMap =
          buildHostMap({ "foo", "bar", "baz" }, { 3, 3, 3 }, { 2, 3, 2 });
        ber = faabric::util::batchExecFactory("bat", "man", 7);
        ber->set_type(BatchExecuteRequest_BatchExecuteType_MIGRATION);
        config.inFlightReqs = buildInFlightReqs(
          ber, 7, { "foo", "foo", "bar", "bar", "bar", "baz", "baz" });
        config.expectedDecision = buildExpectedDecision(
          ber, { "foo", "foo", "bar", "bar", "bar", "baz", "foo" });
    }

    actualDecision = *batchScheduler->makeSchedulingDecision(
      config.hostMap, config.inFlightReqs, ber);
    compareSchedulingDecisions(actualDecision, config.expectedDecision);
}
}
