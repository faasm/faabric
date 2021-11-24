#include <catch2/catch.hpp>

#include "faabric_utils.h"
#include "fixtures.h"

#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/config.h>
#include <faabric/util/func.h>
#include <faabric/util/scheduling.h>

using namespace faabric::util;

namespace tests {

TEST_CASE("Test building scheduling decisions", "[util][scheduling-decisions]")
{
    int appId = 123;
    int groupId = 345;

    std::string hostA = "hostA";
    std::string hostB = "hostB";
    std::string hostC = "hostC";

    auto req = batchExecFactory("foo", "bar", 3);

    SchedulingDecision decision(appId, groupId);

    faabric::Message msgA = req->mutable_messages()->at(0);
    faabric::Message msgB = req->mutable_messages()->at(1);
    faabric::Message msgC = req->mutable_messages()->at(2);

    decision.addMessage(hostB, msgA);
    decision.addMessage(hostA, msgB);
    decision.addMessage(hostC, msgC);

    std::vector<int32_t> expectedMsgIds = { msgA.id(), msgB.id(), msgC.id() };
    std::vector<std::string> expectedHosts = { hostB, hostA, hostC };
    std::vector<int32_t> expectedAppIdxs = { msgA.appidx(),
                                             msgB.appidx(),
                                             msgC.appidx() };

    REQUIRE(decision.appId == appId);
    REQUIRE(decision.groupId == groupId);
    REQUIRE(decision.nFunctions == 3);
    REQUIRE(decision.messageIds == expectedMsgIds);
    REQUIRE(decision.hosts == expectedHosts);
    REQUIRE(decision.appIdxs == expectedAppIdxs);
}

TEST_CASE("Test converting point-to-point mappings to scheduling decisions",
          "[util][scheduling-decisions]")
{
    int appId = 123;
    int groupId = 345;

    int appIdxA = 2;
    int groupIdxA = 22;
    int msgIdA = 222;
    std::string hostA = "foobar";

    int appIdxB = 3;
    int groupIdxB = 33;
    int msgIdB = 333;
    std::string hostB = "bazbaz";

    std::vector<int> expectedAppIdxs = { appIdxA, appIdxB };
    std::vector<int> expectedGroupIdxs = { groupIdxA, groupIdxB };
    std::vector<int> expectedMessageIds = { msgIdA, msgIdB };
    std::vector<std::string> expectedHosts = { hostA, hostB };

    faabric::PointToPointMappings mappings;
    mappings.set_appid(appId);
    mappings.set_groupid(groupId);

    auto* mappingA = mappings.add_mappings();
    mappingA->set_host(hostA);
    mappingA->set_messageid(msgIdA);
    mappingA->set_appidx(appIdxA);
    mappingA->set_groupidx(groupIdxA);

    auto* mappingB = mappings.add_mappings();
    mappingB->set_host(hostB);
    mappingB->set_messageid(msgIdB);
    mappingB->set_appidx(appIdxB);
    mappingB->set_groupidx(groupIdxB);

    auto actual =
      faabric::util::SchedulingDecision::fromPointToPointMappings(mappings);

    REQUIRE(actual.appId == appId);
    REQUIRE(actual.nFunctions == 2);

    REQUIRE(actual.appIdxs == expectedAppIdxs);
    REQUIRE(actual.groupIdxs == expectedGroupIdxs);
    REQUIRE(actual.messageIds == expectedMessageIds);
    REQUIRE(actual.hosts == expectedHosts);
}

class SchedulingDecisionTestFixture : public SchedulerTestFixture
{
  public:
    SchedulingDecisionTestFixture() { faabric::util::setMockMode(true); }

    ~SchedulingDecisionTestFixture() { faabric::util::setMockMode(false); }

  protected:
    int appId = 123;
    int groupId = 456;
    std::string masterHost = faabric::util::getSystemConfig().endpointHost;

    // Helper struct to configure one scheduling decision
    struct SchedulingConfig
    {
        std::vector<std::string> hosts;
        std::vector<int> slots;
        int numReqs;
        bool forceLocal;
        SchedulingTopologyHint topologyHint;
        std::vector<std::string> expectedHosts;
    };

    // Helper method to set the available hosts and slots per host prior to
    // making a scheduling decision
    void setHostResources(std::vector<std::string> registeredHosts,
                          std::vector<int> slotsPerHost)
    {
        assert(registeredHosts.size() == slotsPerHost.size());
        auto& sch = faabric::scheduler::getScheduler();
        sch.clearRecordedMessages();

        for (int i = 0; i < registeredHosts.size(); i++) {
            faabric::HostResources resources;
            resources.set_slots(slotsPerHost.at(i));
            resources.set_usedslots(0);

            sch.addHostToGlobalSet(registeredHosts.at(i));

            // If setting resources for the master host, update the scheduler.
            // Otherwise, queue the resource response
            if (i == 0) {
                sch.setThisHostResources(resources);
            } else {
                faabric::scheduler::queueResourceResponse(registeredHosts.at(i),
                                                          resources);
            }
        }
    }

    // We test the scheduling decision twice: the first one will follow the
    // unregistered hosts path, the second one the registerd hosts one.
    void testActualSchedulingDecision(
      std::shared_ptr<faabric::BatchExecuteRequest> req,
      const SchedulingConfig& config)
    {
        auto& sch = faabric::scheduler::getScheduler();
        SchedulingDecision actualDecision(appId, groupId);

        // Set resources for all hosts
        setHostResources(config.hosts, config.slots);

        // The first time we request the scheduling decision, we will follow the
        // unregistered hosts path
        actualDecision = sch.publicMakeSchedulingDecision(
          req, config.forceLocal, config.topologyHint);
        REQUIRE(actualDecision.hosts == config.expectedHosts);

        // Set resources again to reset the used slots
        setHostResources(config.hosts, config.slots);

        // The second time we request the scheduling decision, we will follow
        // the registered hosts path
        actualDecision = sch.publicMakeSchedulingDecision(
          req, config.forceLocal, config.topologyHint);
        REQUIRE(actualDecision.hosts == config.expectedHosts);
    }
};

TEST_CASE_METHOD(SchedulingDecisionTestFixture,
                 "Test basic scheduling decision",
                 "[util][scheduling-decision]")
{
    SchedulingConfig config = {
        .hosts = { masterHost, "hostA" },
        .slots = { 1, 1 },
        .numReqs = 2,
        .forceLocal = false,
        .topologyHint = SchedulingTopologyHint::NORMAL,
        .expectedHosts = { masterHost, "hostA" },
    };

    auto req = batchExecFactory("foo", "bar", config.numReqs);

    testActualSchedulingDecision(req, config);
}

TEST_CASE_METHOD(SchedulingDecisionTestFixture,
                 "Test overloading all resources defaults to master",
                 "[util][scheduling-decision]")
{
    SchedulingConfig config = {
        .hosts = { masterHost, "hostA" },
        .slots = { 1, 1 },
        .numReqs = 3,
        .forceLocal = false,
        .topologyHint = SchedulingTopologyHint::NORMAL,
        .expectedHosts = { masterHost, "hostA", masterHost },
    };

    auto req = batchExecFactory("foo", "bar", config.numReqs);

    testActualSchedulingDecision(req, config);
}

TEST_CASE_METHOD(SchedulingDecisionTestFixture,
                 "Test force local forces executing at master",
                 "[util][scheduling-decision]")
{
    SchedulingConfig config = {
        .hosts = { masterHost, "hostA" },
        .slots = { 1, 1 },
        .numReqs = 2,
        .forceLocal = false,
        .topologyHint = SchedulingTopologyHint::NORMAL,
        .expectedHosts = { masterHost, "hostA" },
    };

    auto req = batchExecFactory("foo", "bar", config.numReqs);

    SECTION("Force local off")
    {
        config.forceLocal = false;
        config.expectedHosts = { masterHost, "hostA" };
    }

    SECTION("Force local on")
    {
        config.forceLocal = true;
        config.expectedHosts = { masterHost, masterHost };
    }

    testActualSchedulingDecision(req, config);
}

TEST_CASE_METHOD(SchedulingDecisionTestFixture,
                 "Test master running out of resources",
                 "[util][scheduling-decision]")
{
    SchedulingConfig config = {
        .hosts = { masterHost, "hostA" },
        .slots = { 0, 2 },
        .numReqs = 2,
        .forceLocal = false,
        .topologyHint = SchedulingTopologyHint::NORMAL,
        .expectedHosts = { "hostA", "hostA" },
    };

    auto req = batchExecFactory("foo", "bar", config.numReqs);

    testActualSchedulingDecision(req, config);
}

TEST_CASE_METHOD(SchedulingDecisionTestFixture,
                 "Test scheduling decision with many requests",
                 "[util][scheduling-decision]")
{
    SchedulingConfig config = {
        .hosts = { masterHost, "hostA", "hostB", "hostC" },
        .slots = { 0, 0, 0, 0 },
        .numReqs = 8,
        .forceLocal = false,
        .topologyHint = SchedulingTopologyHint::NORMAL,
        .expectedHosts = { masterHost, masterHost, masterHost, masterHost },
    };

    auto req = batchExecFactory("foo", "bar", config.numReqs);

    SECTION("Even slot distribution across hosts")
    {
        config.slots = { 2, 2, 2, 2 };
        config.expectedHosts = { masterHost, masterHost, "hostA", "hostA",
                                 "hostB",    "hostB",    "hostC", "hostC" };
    }

    SECTION("Uneven slot ditribution across hosts")
    {
        config.slots = { 3, 2, 2, 1 };
        config.expectedHosts = { masterHost, masterHost, masterHost, "hostA",
                                 "hostA",    "hostB",    "hostB",    "hostC" };
    }

    SECTION("Very uneven slot distribution across hosts")
    {
        config.slots = { 1, 0, 0, 0 };
        config.expectedHosts = {
            masterHost, masterHost, masterHost, masterHost,
            masterHost, masterHost, masterHost, masterHost
        };
    }

    testActualSchedulingDecision(req, config);
}

TEST_CASE_METHOD(SchedulingDecisionTestFixture,
                 "Test sticky pairs scheduling topology hint",
                 "[util][scheduling-decision]")
{
    SchedulingConfig config = {
        .hosts = { masterHost, "hostA" },
        .slots = { 1, 1 },
        .numReqs = 2,
        .forceLocal = false,
        .topologyHint = SchedulingTopologyHint::PAIRS,
        .expectedHosts = { masterHost, "hostA" },
    };

    std::shared_ptr<faabric::BatchExecuteRequest> req;

    SECTION("Test with hint we only schedule to new hosts pairs of requests")
    {
        config.expectedHosts = { masterHost, masterHost };
        req = batchExecFactory("foo", "bar", config.numReqs);
    }

    SECTION("Test hint does not apply for master requests")
    {
        config.slots = { 0, 1 };
        config.numReqs = 1;
        config.expectedHosts = { "hostA" };
        req = batchExecFactory("foo", "bar", config.numReqs);
    }

    SECTION("Test with hint we may overload remote hosts")
    {
        config.hosts = { masterHost, "hostA", "hostB" };
        config.numReqs = 5;
        config.slots = { 2, 2, 1 };
        config.expectedHosts = {
            masterHost, masterHost, "hostA", "hostA", "hostA"
        };
        req = batchExecFactory("foo", "bar", config.numReqs);
    }

    SECTION("Test with hint we still overload master if running out of slots")
    {
        config.hosts = { masterHost, "hostA" };
        config.numReqs = 5;
        config.slots = { 2, 2 };
        config.expectedHosts = {
            masterHost, masterHost, "hostA", "hostA", masterHost
        };
        req = batchExecFactory("foo", "bar", config.numReqs);
    }

    SECTION("Test hint with uneven slot distribution")
    {
        config.hosts = { masterHost, "hostA" };
        config.numReqs = 5;
        config.slots = { 2, 3 };
        config.expectedHosts = {
            masterHost, masterHost, "hostA", "hostA", "hostA"
        };
        req = batchExecFactory("foo", "bar", config.numReqs);
    }

    SECTION("Test hint with uneven slot distribution and overload")
    {
        config.hosts = { masterHost, "hostA", "hostB" };
        config.numReqs = 6;
        config.slots = { 2, 3, 1 };
        config.expectedHosts = { masterHost, masterHost, "hostA",
                                 "hostA",    "hostA",    "hostA" };
        req = batchExecFactory("foo", "bar", config.numReqs);
    }

    testActualSchedulingDecision(req, config);
}
}
