#include <catch.hpp>

#include "faabric/proto/faabric.pb.h"
#include "faabric/util/func.h"
#include "faabric_utils.h"

#include <faabric/redis/Redis.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/environment.h>

using namespace scheduler;
using namespace redis;

namespace tests {
TEST_CASE("Test scheduler available hosts", "[scheduler]")
{
    cleanFaabric();

    faabric::scheduler::Scheduler& sch = faabric::scheduler::getScheduler();

    // Set up some available hosts
    std::string thisHost = faabric::util::getSystemConfig().endpointHost;
    std::string hostA = "hostA";
    std::string hostB = "hostB";
    std::string hostC = "hostC";

    sch.addHostToGlobalSet(hostA);
    sch.addHostToGlobalSet(hostB);
    sch.addHostToGlobalSet(hostC);

    std::unordered_set<std::string> expectedHosts = {
        thisHost, hostA, hostB, hostC
    };
    std::unordered_set<std::string> actualHosts = sch.getAvailableHosts();

    REQUIRE(actualHosts == expectedHosts);

    sch.removeHostFromGlobalSet(hostB);
    sch.removeHostFromGlobalSet(hostC);

    expectedHosts = { thisHost, hostA };
    actualHosts = sch.getAvailableHosts();

    REQUIRE(actualHosts == expectedHosts);
}

TEST_CASE("Test scheduling on this host", "[scheduler]")
{
    cleanFaabric();

    std::string thisHost = faabric::util::getSystemConfig().endpointHost;

    faabric::scheduler::Scheduler& sch = faabric::scheduler::getScheduler();

    int nCalls = 10;
    faabric::HostResources res;
    res.set_cores(nCalls);
    sch.setThisHostResources(res);

    std::vector<faabric::Message> msgs;
    std::vector<std::string> expectedHosts;
    for (int i = 0; i < nCalls; i++) {
        faabric::Message msg = faabric::util::messageFactory("foo", "bar");

        // Set important bind fields
        msg.set_ispython(true);
        msg.set_pythonfunction("baz");
        msg.set_pythonuser("foobar");
        msg.set_issgx(true);

        msgs.push_back(msg);

        expectedHosts.push_back(thisHost);
    }

    faabric::BatchExecuteRequest req = faabric::util::batchExecFactory(msgs);
    std::vector<std::string> actualHosts = sch.callFunctions(req);

    REQUIRE(actualHosts == expectedHosts);

    // Check the scheduler info
    faabric::Message firstMsg = msgs.at(0);
    REQUIRE(sch.getFunctionInFlightCount(firstMsg) == nCalls);
    REQUIRE(sch.getFunctionFaasletCount(firstMsg) == nCalls);

    // Check the bind messages
    auto bindQueue = sch.getBindQueue();
    REQUIRE(bindQueue->size() == nCalls);
    for (int i = 0; i < nCalls; i++) {
        faabric::Message msg = bindQueue->dequeue();
        REQUIRE(msg.user() == firstMsg.user());
        REQUIRE(msg.function() == firstMsg.function());
        REQUIRE(msg.type() == faabric::Message_MessageType_BIND);
        REQUIRE(msg.ispython());
        REQUIRE(msg.pythonuser() == "foobar");
        REQUIRE(msg.pythonfunction() == "baz");
        REQUIRE(msg.issgx());
    }
}
}
