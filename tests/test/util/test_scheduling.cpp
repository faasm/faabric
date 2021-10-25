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

    std::string hostA = "hostA";
    std::string hostB = "hostB";
    std::string hostC = "hostC";

    auto req = batchExecFactory("foo", "bar", 3);

    SchedulingDecision decision(appId);

    faabric::Message msgA = req->mutable_messages()->at(0);
    faabric::Message msgB = req->mutable_messages()->at(1);
    faabric::Message msgC = req->mutable_messages()->at(2);

    decision.addMessage(hostB, msgA);
    decision.addMessage(hostA, msgB);
    decision.addMessage(hostC, msgC);

    std::vector<int32_t> expectedMsgIds = { msgA.id(), msgB.id(), msgC.id() };
    std::vector<std::string> expectedHosts = { hostB, hostA, hostC };
    std::vector<int32_t> expectedAppIdxs = { msgA.appindex(),
                                             msgB.appindex(),
                                             msgC.appindex() };

    REQUIRE(decision.appId == appId);
    REQUIRE(decision.nFunctions == 3);
    REQUIRE(decision.messageIds == expectedMsgIds);
    REQUIRE(decision.hosts == expectedHosts);
    REQUIRE(decision.appIdxs == expectedAppIdxs);
}

TEST_CASE("Test converting point-to-point mappings to scheduling decisions",
          "[util]")
{
    int appId = 123;

    int idxA = 22;
    int msgIdA = 222;
    std::string hostA = "foobar";

    int idxB = 33;
    int msgIdB = 333;
    std::string hostB = "bazbaz";

    std::vector<int> expectedIdxs = { idxA, idxB };
    std::vector<int> expectedMessageIds = { msgIdA, msgIdB };
    std::vector<std::string> expectedHosts = { hostA, hostB };

    faabric::PointToPointMappings mappings;
    mappings.set_groupid(appId);

    auto* mappingA = mappings.add_mappings();
    mappingA->set_host(hostA);
    mappingA->set_messageid(msgIdA);
    mappingA->set_recvidx(idxA);

    auto* mappingB = mappings.add_mappings();
    mappingB->set_host(hostB);
    mappingB->set_messageid(msgIdB);
    mappingB->set_recvidx(idxB);

    auto actual =
      faabric::util::SchedulingDecision::fromPointToPointMappings(mappings);

    REQUIRE(actual.appId == appId);
    REQUIRE(actual.nFunctions == 2);

    REQUIRE(actual.appIdxs == expectedIdxs);
    REQUIRE(actual.messageIds == expectedMessageIds);
    REQUIRE(actual.hosts == expectedHosts);
}
}
