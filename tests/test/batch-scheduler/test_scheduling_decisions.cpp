#include <catch2/catch.hpp>

#include "faabric_utils.h"
#include "fixtures.h"

#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/util/batch.h>

using namespace faabric::batch_scheduler;

namespace tests {

TEST_CASE_METHOD(ConfFixture, "Test building scheduling decisions", "[util]")
{
    int appId = 123;
    int groupId = 345;

    std::string hostA = "hostA";
    std::string hostB = "hostB";
    std::string hostC = "hostC";

    std::string thisHost = conf.endpointHost;
    std::set<std::string> expectedUniqueHosts;

    bool expectSingleHost = false;
    SECTION("Multi-host")
    {
        expectedUniqueHosts = { hostA, hostB, hostC };
    }

    SECTION("Only remote hosts")
    {
        hostB = "hostA";
        hostC = "hostA";

        expectSingleHost = true;
        expectedUniqueHosts = { "hostA" };
    }

    SECTION("All this host")
    {
        hostA = thisHost;
        hostB = thisHost;
        hostC = thisHost;

        expectSingleHost = true;
        expectedUniqueHosts = { thisHost };
    }

    auto req = faabric::util::batchExecFactory("foo", "bar", 3);

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
    REQUIRE(decision.uniqueHosts() == expectedUniqueHosts);
    REQUIRE(decision.appIdxs == expectedAppIdxs);
    REQUIRE(decision.isSingleHost() == expectSingleHost);

    // We can compare decisions with the == operator
    auto newDecision = decision;
    REQUIRE(newDecision == decision);
    newDecision.groupId = 1338;
    REQUIRE(newDecision != decision);

    // We can print scheduling decisions
    REQUIRE_NOTHROW(decision.print());
}

TEST_CASE("Test converting point-to-point mappings to scheduling decisions",
          "[util]")
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

    auto actual = SchedulingDecision::fromPointToPointMappings(mappings);

    REQUIRE(actual.appId == appId);
    REQUIRE(actual.nFunctions == 2);

    REQUIRE(actual.appIdxs == expectedAppIdxs);
    REQUIRE(actual.groupIdxs == expectedGroupIdxs);
    REQUIRE(actual.messageIds == expectedMessageIds);
    REQUIRE(actual.hosts == expectedHosts);
}

TEST_CASE("Test removing a message from a scheduling decision",
          "[batch-scheduler]")
{
    // Build a scheduling decision
    auto req = faabric::util::batchExecFactory("foo", "bar", 3);
    SchedulingDecision decision(req->appid(), req->groupid());
    decision.addMessage("foo", req->messages(0));
    decision.addMessage("bar", req->messages(1));
    decision.addMessage("baz", req->messages(2));

    // Record the original values
    int nFunctions = decision.nFunctions;
    int nHosts = decision.hosts.size();
    int nMessageIds = decision.messageIds.size();
    int nAppIdxs = decision.appIdxs.size();
    int nGroupIdxs = decision.groupIdxs.size();

    // Remove message from scheduling decision
    decision.removeMessage(req->messages(1).id());

    // Check decision after removal
    REQUIRE(decision.nFunctions == (nFunctions - 1));
    REQUIRE(decision.hosts.size() == (nHosts - 1));
    REQUIRE(decision.messageIds.size() == (nMessageIds - 1));
    REQUIRE(decision.appIdxs.size() == (nAppIdxs - 1));
    REQUIRE(decision.groupIdxs.size() == (nGroupIdxs - 1));

    // Removing a non-existant id throws an exception
    REQUIRE_THROWS(decision.removeMessage(req->messages(1).id()));

    // Lastly, drain the decision and check again
    decision.removeMessage(req->messages(0).id());
    decision.removeMessage(req->messages(2).id());
    REQUIRE(decision.nFunctions == 0);
    REQUIRE(decision.hosts.empty());
    REQUIRE(decision.messageIds.empty());
    REQUIRE(decision.appIdxs.empty());
    REQUIRE(decision.groupIdxs.empty());
}

TEST_CASE_METHOD(CachedDecisionTestFixture,
                 "Test caching scheduling decisions",
                 "[util]")
{
    int appId = 123;
    int groupId = 345;

    std::string thisHost = faabric::util::getSystemConfig().endpointHost;

    auto req = faabric::util::batchExecFactory("foo", "bar", 5);
    std::vector<std::string> hosts = {
        "alpha", "alpha", "beta", "gamma", "alpha",
    };

    // Build the decision
    SchedulingDecision decision(appId, groupId);
    for (int i = 0; i < hosts.size(); i++) {
        decision.addMessage(hosts.at(i), req->messages().at(i));
    }

    // Check no decision to start with
    REQUIRE(decisionCache.getCachedDecision(req) == nullptr);

    // Cache the decision
    decisionCache.addCachedDecision(req, decision);

    // Get the cached decision
    std::shared_ptr<CachedDecision> actual =
      decisionCache.getCachedDecision(req);
    REQUIRE(actual != nullptr);
    REQUIRE(actual->getHosts() == hosts);
    REQUIRE(actual->getGroupId() == groupId);

    // Check clearing decisions
    decisionCache.clear();
    REQUIRE(decisionCache.getCachedDecision(req) == nullptr);
}

TEST_CASE_METHOD(CachedDecisionTestFixture,
                 "Test caching invalid decision causes error",
                 "[util]")
{
    auto req = faabric::util::batchExecFactory("foo", "bar", 3);

    // Decision with wrong number of hosts
    std::vector<std::string> hosts = { "alpha", "alpha" };
    SchedulingDecision decision(123, 345);
    for (int i = 0; i < hosts.size(); i++) {
        decision.addMessage(hosts.at(i), req->messages().at(i));
    }

    // Check it throws an error
    REQUIRE_THROWS(decisionCache.addCachedDecision(req, decision));
}

TEST_CASE_METHOD(CachedDecisionTestFixture,
                 "Test caching multiple decisions for same function",
                 "[util]")
{
    auto reqA = faabric::util::batchExecFactory("foo", "bar", 3);
    auto reqB = faabric::util::batchExecFactory("foo", "bar", 5);

    // Decision with wrong number of hosts
    std::vector<std::string> hostsA = { "alpha", "alpha", "beta" };
    std::vector<std::string> hostsB = {
        "alpha", "alpha", "beta", "gamma", "gamma"
    };

    SchedulingDecision decisionA(123, 345);
    for (int i = 0; i < hostsA.size(); i++) {
        decisionA.addMessage(hostsA.at(i), reqA->messages().at(i));
    }

    SchedulingDecision decisionB(456, 789);
    for (int i = 0; i < hostsB.size(); i++) {
        decisionB.addMessage(hostsB.at(i), reqB->messages().at(i));
    }

    // Add both
    decisionCache.addCachedDecision(reqA, decisionA);
    decisionCache.addCachedDecision(reqB, decisionB);

    // Get both
    std::shared_ptr<CachedDecision> actualA =
      decisionCache.getCachedDecision(reqA);
    std::shared_ptr<CachedDecision> actualB =
      decisionCache.getCachedDecision(reqB);

    REQUIRE(actualA->getHosts() == hostsA);
    REQUIRE(actualA->getGroupId() == 345);
    REQUIRE(actualB->getHosts() == hostsB);
    REQUIRE(actualB->getGroupId() == 789);
}
}
