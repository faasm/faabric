#include <catch.hpp>

#include <faabric/proto/faabric.pb.h>
#include <faabric/redis/Redis.h>
#include <faabric/scheduler/FunctionCallClient.h>
#include <faabric/scheduler/FunctionCallServer.h>
#include <faabric/scheduler/MpiWorld.h>
#include <faabric/scheduler/MpiWorldRegistry.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/config.h>
#include <faabric/util/environment.h>
#include <faabric/util/func.h>
#include <faabric/util/network.h>
#include <faabric/util/testing.h>
#include <faabric_utils.h>

using namespace scheduler;

namespace tests {

TEST_CASE("Test sending MPI message", "[new-scheduler]")
{
    cleanFaabric();

    // Start the server
    faabric::transport::MessageContext context;
    FunctionCallServer server;
    server.start(context);
    usleep(1000 * 100);

    // Create an MPI world on this host and one on a "remote" host
    std::string otherHost = "192.168.9.2";

    const char* user = "mpi";
    const char* func = "hellompi";
    const faabric::Message& msg = faabric::util::messageFactory(user, func);
    int worldId = 123;
    int worldSize = 2;

    scheduler::MpiWorldRegistry& registry = getMpiWorldRegistry();
    scheduler::MpiWorld& localWorld = registry.createWorld(msg, worldId);
    localWorld.overrideHost(LOCALHOST);
    localWorld.create(msg, worldId, worldSize);

    scheduler::MpiWorld remoteWorld;
    remoteWorld.overrideHost(otherHost);
    remoteWorld.initialiseFromState(msg, worldId);

    // Register a rank on each
    int rankLocal = 0;
    int rankRemote = 1;
    localWorld.registerRank(rankLocal);
    remoteWorld.registerRank(rankRemote);

    // Create a message
    faabric::MPIMessage mpiMsg;
    mpiMsg.set_worldid(worldId);
    mpiMsg.set_sender(rankRemote);
    mpiMsg.set_destination(rankLocal);

    // Send the message
    FunctionCallClient cli(context, LOCALHOST);
    cli.sendMPIMessage(std::make_shared<faabric::MPIMessage>(mpiMsg));
    usleep(1000 * 100);

    // Make sure the message has been put on the right queue locally
    std::shared_ptr<InMemoryMpiQueue> queue =
      localWorld.getLocalQueue(rankRemote, rankLocal);
    REQUIRE(queue->size() == 1);
    const std::shared_ptr<faabric::MPIMessage> actualMessage = queue->dequeue();

    REQUIRE(actualMessage->worldid() == worldId);
    REQUIRE(actualMessage->sender() == rankRemote);

    // First close the client
    cli.close();
    // Stop the server and messaging context
    server.stop(context);
}

TEST_CASE("Test sending flush message", "[new-scheduler]")
{
    cleanFaabric();

    // Start the server
    faabric::transport::MessageContext context;
    FunctionCallServer server;
    server.start(context);
    usleep(1000 * 100);

    // Set up some state
    faabric::state::State& state = faabric::state::getGlobalState();
    state.getKV("demo", "blah", 10);
    state.getKV("other", "foo", 30);

    REQUIRE(state.getKVCount() == 2);

    // Execute a couple of functions
    faabric::Message msgA = faabric::util::messageFactory("dummy", "foo");
    faabric::Message msgB = faabric::util::messageFactory("dummy", "bar");
    faabric::scheduler::Scheduler& sch = faabric::scheduler::getScheduler();
    sch.callFunction(msgA);
    sch.callFunction(msgB);

    // Empty the queued messages
    auto bindQueue = sch.getBindQueue();
    auto functionQueueA = sch.getFunctionQueue(msgA);
    auto functionQueueB = sch.getFunctionQueue(msgB);

    REQUIRE(bindQueue->size() == 2);
    REQUIRE(functionQueueA->size() == 1);
    REQUIRE(functionQueueB->size() == 1);

    bindQueue->dequeue();
    bindQueue->dequeue();
    functionQueueA->dequeue();
    functionQueueB->dequeue();

    // Background threads to get flush messages
    std::thread tA([&functionQueueA] {
        faabric::Message msg = functionQueueA->dequeue(1000);
        REQUIRE(msg.type() == faabric::Message_MessageType_FLUSH);
    });

    std::thread tB([&functionQueueB] {
        faabric::Message msg = functionQueueB->dequeue(1000);
        REQUIRE(msg.type() == faabric::Message_MessageType_FLUSH);
    });

    // Send flush message
    FunctionCallClient cli(context, LOCALHOST);
    cli.sendFlush();

    // Wait for thread to get flush message
    if (tA.joinable()) {
        tA.join();
    }

    if (tB.joinable()) {
        tB.join();
    }

    // Close the client, and then stop the server
    cli.close();
    server.stop(context);

    // Check the scheduler has been flushed
    REQUIRE(sch.getFunctionRegisteredHostCount(msgA) == 0);
    REQUIRE(sch.getFunctionRegisteredHostCount(msgB) == 0);

    // Check state has been cleared
    REQUIRE(state.getKVCount() == 0);
}

TEST_CASE("Test broadcasting flush message", "[new-scheduler]")
{
    cleanFaabric();
    faabric::util::setMockMode(true);

    // Add hosts to global set
    faabric::scheduler::Scheduler& sch = faabric::scheduler::getScheduler();

    std::string hostA = "alpha";
    std::string hostB = "beta";
    std::string hostC = "gamma";

    std::vector<std::string> expectedHosts = { hostA, hostB, hostC };

    sch.addHostToGlobalSet(hostA);
    sch.addHostToGlobalSet(hostB);
    sch.addHostToGlobalSet(hostC);

    // Broadcast the flush
    sch.broadcastFlush();

    // Make sure messages have been sent
    auto calls = faabric::scheduler::getFlushCalls();
    REQUIRE(calls.size() == 3);

    std::vector<std::string> actualHosts;
    for (auto c : calls) {
        actualHosts.emplace_back(c.first);
    }

    faabric::util::setMockMode(false);
}

TEST_CASE("Test client batch execution request", "[new-scheduler]")
{
    cleanFaabric();

    // Start the server
    faabric::transport::MessageContext context;
    FunctionCallServer server;
    server.start(context);
    usleep(1000 * 100);

    // Set up a load of calls
    int nCalls = 30;
    std::vector<faabric::Message> msgs;
    for (int i = 0; i < nCalls; i++) {
        faabric::Message msg = faabric::util::messageFactory("foo", "bar");
        msgs.emplace_back(msg);
    }

    // Make the request
    faabric::BatchExecuteRequest req = faabric::util::batchExecFactory(msgs);
    FunctionCallClient cli(context, LOCALHOST);
    cli.executeFunctions(req);
    usleep(1000 * 100);

    // Close the client
    cli.close();
    // Stop the server
    server.stop(context);

    faabric::Message m = msgs.at(0);
    faabric::scheduler::Scheduler& sch = faabric::scheduler::getScheduler();

    // Check no other hosts have been registered
    REQUIRE(sch.getFunctionRegisteredHostCount(m) == 0);

    // Check we've got faaslets and in-flight messages
    REQUIRE(sch.getFunctionInFlightCount(m) == nCalls);
    REQUIRE(sch.getFunctionFaasletCount(m) == nCalls);

    REQUIRE(sch.getBindQueue()->size() == nCalls);
}

/*
TEST_CASE("Test get resources request", "[scheduler]")
{
    cleanFaabric();

    faabric::scheduler::Scheduler& sch = faabric::scheduler::getScheduler();

    int expectedCores;
    int expectedExecutors;
    int expectedInFlight;

    SECTION("Override resources")
    {
        faabric::HostResources res;

        expectedCores = 10;
        expectedExecutors = 15;
        expectedInFlight = 20;

        res.set_boundexecutors(expectedExecutors);
        res.set_cores(expectedCores);
        res.set_functionsinflight(expectedInFlight);

        sch.setThisHostResources(res);
    }
    SECTION("Default resources")
    {
        expectedCores = sch.getThisHostResources().cores();
        expectedExecutors = 0;
        expectedInFlight = 0;
    }

    // Start the server
    faabric::transport::MessageContext context;
    FunctionCallServer server;
    server.start(context);
    usleep(1000 * 100);

    // Make the request
    faabric::ResourceRequest req;
    FunctionCallClient cli(LOCALHOST);
    faabric::HostResources resResponse = cli.getResources(req);

    REQUIRE(resResponse.boundexecutors() == expectedExecutors);
    REQUIRE(resResponse.cores() == expectedCores);
    REQUIRE(resResponse.functionsinflight() == expectedInFlight);

    // Stop the server
    server.stop(context);
}
*/

TEST_CASE("Test unregister request", "[scheduler]")
{
    cleanFaabric();

    faabric::util::setMockMode(true);
    std::string otherHost = "other";

    // Remove capacity from this host and add on other
    faabric::HostResources thisResources;
    faabric::HostResources otherResources;
    thisResources.set_cores(0);
    otherResources.set_cores(5);

    faabric::scheduler::Scheduler& sch = faabric::scheduler::getScheduler();
    sch.setThisHostResources(thisResources);
    faabric::scheduler::queueResourceResponse(otherHost, otherResources);

    // Request a function and check the other host is registered
    faabric::Message msg = faabric::util::messageFactory("foo", "bar");
    sch.addHostToGlobalSet(otherHost);
    sch.callFunction(msg);

    REQUIRE(sch.getFunctionRegisteredHostCount(msg) == 1);

    faabric::util::setMockMode(false);
    faabric::scheduler::clearMockRequests();

    // Start the server
    faabric::transport::MessageContext context;
    FunctionCallServer server;
    server.start(context);
    usleep(1000 * 100);

    // Make the request with a host that's not registered
    faabric::UnregisterRequest reqA;
    reqA.set_host("foobar");
    *reqA.mutable_function() = msg;

    FunctionCallClient cli(context, LOCALHOST);
    cli.unregister(reqA);
    REQUIRE(sch.getFunctionRegisteredHostCount(msg) == 1);

    // Make the request to unregister the actual host
    faabric::UnregisterRequest reqB;
    reqB.set_host(otherHost);
    *reqB.mutable_function() = msg;
    cli.unregister(reqB);
    REQUIRE(sch.getFunctionRegisteredHostCount(msg) == 0);

    // Stop the server
    cli.close();
    server.stop(context);
}
}
