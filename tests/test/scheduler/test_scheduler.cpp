#include <catch.hpp>

#include "faabric/proto/faabric.pb.h"
#include "faabric/scheduler/FunctionCallClient.h"
#include "faabric/util/func.h"
#include "faabric/util/testing.h"
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

TEST_CASE("Test batch scheduling", "[scheduler]")
{
    cleanFaabric();

    // Mock everything
    faabric::util::setMockMode(true);

    std::string thisHost = faabric::util::getSystemConfig().endpointHost;

    faabric::scheduler::Scheduler& sch = faabric::scheduler::getScheduler();

    // Set up another host
    std::string hostB = "beta";
    sch.addHostToGlobalSet(hostB);

    int nCalls = 10;
    int coresA = 5;
    int coresB = 6;
    int nCallsB = nCalls - coresA;

    faabric::HostResources resA;
    resA.set_cores(coresA);

    faabric::HostResources resB;
    resB.set_cores(coresB);

    // Prepare host resources
    sch.setThisHostResources(resA);
    faabric::scheduler::queueResourceResponse(hostB, resB);

    // Set up the messages
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

        // Expect this host to handle up to its number of cores
        if (i < coresA) {
            expectedHosts.push_back(thisHost);
        } else {
            expectedHosts.push_back(hostB);
        }
    }

    // Create the batch request
    faabric::BatchExecuteRequest req = faabric::util::batchExecFactory(msgs);

    // Schedule the functions
    std::vector<std::string> actualHosts = sch.callFunctions(req);

    // Check resource requests have been made to other host
    auto resRequests = faabric::scheduler::getResourceRequests();
    REQUIRE(resRequests.size() == 1);
    REQUIRE(resRequests.at(0).first == hostB);

    // Check scheduled on expected hosts
    REQUIRE(actualHosts == expectedHosts);

    // Check the scheduler info on this host
    faabric::Message firstMsg = msgs.at(0);
    REQUIRE(sch.getFunctionInFlightCount(firstMsg) == coresA);
    REQUIRE(sch.getFunctionFaasletCount(firstMsg) == coresA);

    // Check the bind messages on this host
    auto bindQueue = sch.getBindQueue();
    REQUIRE(bindQueue->size() == coresA);
    for (int i = 0; i < coresA; i++) {
        faabric::Message msg = bindQueue->dequeue();
        REQUIRE(msg.user() == firstMsg.user());
        REQUIRE(msg.function() == firstMsg.function());
        REQUIRE(msg.type() == faabric::Message_MessageType_BIND);
        REQUIRE(msg.ispython());
        REQUIRE(msg.pythonuser() == "foobar");
        REQUIRE(msg.pythonfunction() == "baz");
        REQUIRE(msg.issgx());
    }

    // Check the message is dispatched to the other host
    auto batchRequests = faabric::scheduler::getBatchRequests();
    REQUIRE(batchRequests.size() == 1);
    auto p = batchRequests.at(0);
    REQUIRE(p.first == hostB);

    // Check the request to the other host
    faabric::BatchExecuteRequest reqB = p.second;
    REQUIRE(reqB.messages_size() == nCallsB);

    faabric::util::setMockMode(false);
}
}
