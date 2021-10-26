#include <catch.hpp>

#include "faabric_utils.h"
#include "fixtures.h"

#include <faabric/util/func.h>
#include <faabric/util/scheduling.h>

using namespace faabric::util;

namespace tests {

TEST_CASE("Test building scheduling decisions", "[util]")
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
          "[util]")
{
    int appId = 123;

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
    mappings.set_groupid(appId);

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
}
