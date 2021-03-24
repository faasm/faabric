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
    std::string otherHost = "beta";
    sch.addHostToGlobalSet(otherHost);

    int nCallsOne = 10;
    int nCallsTwo = 5;
    int thisCores = 5;
    int otherCores = 11;
    int nCallsOffloadedOne = nCallsOne - thisCores;

    faabric::HostResources thisResources;
    thisResources.set_cores(thisCores);

    faabric::HostResources otherResources;
    otherResources.set_cores(otherCores);

    // Prepare two resource responses for other host
    sch.setThisHostResources(thisResources);
    faabric::scheduler::queueResourceResponse(otherHost, otherResources);

    // Set up the messages
    std::vector<faabric::Message> msgsOne;
    std::vector<std::string> expectedHostsOne;
    for (int i = 0; i < nCallsOne; i++) {
        faabric::Message msg = faabric::util::messageFactory("foo", "bar");

        // Set important bind fields
        msg.set_ispython(true);
        msg.set_pythonfunction("baz");
        msg.set_pythonuser("foobar");
        msg.set_issgx(true);

        msgsOne.push_back(msg);

        // Expect this host to handle up to its number of cores
        if (i < thisCores) {
            expectedHostsOne.push_back(thisHost);
        } else {
            expectedHostsOne.push_back(otherHost);
        }
    }

    // Create the batch request
    faabric::BatchExecuteRequest reqOne =
      faabric::util::batchExecFactory(msgsOne);

    // Schedule the functions
    std::vector<std::string> actualHostsOne = sch.callFunctions(reqOne);

    // Check resource requests have been made to other host
    auto resRequestsOne = faabric::scheduler::getResourceRequests();
    REQUIRE(resRequestsOne.size() == 1);
    REQUIRE(resRequestsOne.at(0).first == otherHost);

    // Check scheduled on expected hosts
    REQUIRE(actualHostsOne == expectedHostsOne);

    // Check the scheduler info on this host
    faabric::Message m = msgsOne.at(0);
    REQUIRE(sch.getFunctionInFlightCount(m) == thisCores);
    REQUIRE(sch.getFunctionFaasletCount(m) == thisCores);

    // Check the bind messages on this host
    auto bindQueue = sch.getBindQueue();
    REQUIRE(bindQueue->size() == thisCores);
    for (int i = 0; i < thisCores; i++) {
        faabric::Message msg = bindQueue->dequeue();

        REQUIRE(msg.user() == m.user());
        REQUIRE(msg.function() == m.function());
        REQUIRE(msg.type() == faabric::Message_MessageType_BIND);
        REQUIRE(msg.ispython());
        REQUIRE(msg.pythonuser() == "foobar");
        REQUIRE(msg.pythonfunction() == "baz");
        REQUIRE(msg.issgx());
    }

    // Check the message is dispatched to the other host
    auto batchRequestsOne = faabric::scheduler::getBatchRequests();
    REQUIRE(batchRequestsOne.size() == 1);
    auto batchRequestOne = batchRequestsOne.at(0);
    REQUIRE(batchRequestOne.first == otherHost);

    // Check the request to the other host
    REQUIRE(batchRequestOne.second.messages_size() == nCallsOffloadedOne);
    REQUIRE(batchRequestOne.second.masterhost() == thisHost);

    // Clear mocks
    faabric::scheduler::clearMockRequests();

    // Set up resource response again
    faabric::scheduler::queueResourceResponse(otherHost, otherResources);

    // Now schedule a second batch and check they're also sent to the other host
    // (which is now warm)
    std::vector<faabric::Message> msgsTwo;
    std::vector<std::string> expectedHostsTwo;

    for (int i = 0; i < nCallsTwo; i++) {
        faabric::Message msg = faabric::util::messageFactory("foo", "bar");
        msgsTwo.push_back(msg);
        expectedHostsTwo.push_back(otherHost);
    }

    // Create the batch request
    faabric::BatchExecuteRequest reqTwo =
      faabric::util::batchExecFactory(msgsTwo);

    // Schedule the functions
    std::vector<std::string> actualHostsTwo = sch.callFunctions(reqTwo);

    // Check resource request made again
    auto resRequestsTwo = faabric::scheduler::getResourceRequests();
    REQUIRE(resRequestsTwo.size() == 1);
    REQUIRE(resRequestsTwo.at(0).first == otherHost);

    // Check scheduled on expected hosts
    REQUIRE(actualHostsTwo == expectedHostsTwo);

    // Check no other functions have been scheduled on this host
    REQUIRE(sch.getFunctionInFlightCount(m) == thisCores);
    REQUIRE(sch.getFunctionFaasletCount(m) == thisCores);

    // Check the second message is dispatched to the other host
    auto batchRequestsTwo = faabric::scheduler::getBatchRequests();
    REQUIRE(batchRequestsTwo.size() == 1);
    auto pTwo = batchRequestsTwo.at(0);
    REQUIRE(pTwo.first == otherHost);

    // Check the request to the other host
    REQUIRE(pTwo.second.messages_size() == nCallsTwo);
    REQUIRE(pTwo.second.masterhost() == thisHost);

    faabric::util::setMockMode(false);
}
}
