#include <catch.hpp>

#include "faabric_utils.h"

#include <faabric/redis/Redis.h>
#include <faabric/scheduler/FunctionCallClient.h>
#include <faabric/scheduler/FunctionCallServer.h>
#include <faabric/scheduler/MpiWorld.h>
#include <faabric/scheduler/MpiWorldRegistry.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/config.h>
#include <faabric/util/func.h>
#include <faabric/util/network.h>

using namespace scheduler;

namespace tests {
TEST_CASE("Test sending function call", "[scheduler]")
{
    cleanFaabric();

    // Start the server
    ServerContext serverContext;
    FunctionCallServer server;
    server.start();
    usleep(1000 * 100);

    // Create a message
    faabric::Message msg = faabric::util::messageFactory("foo", "bar");
    msg.set_inputdata("foobarbaz");

    // Get the queue for the function
    Scheduler& sch = scheduler::getScheduler();
    std::shared_ptr<InMemoryMessageQueue> funcQueue = sch.getFunctionQueue(msg);

    // Check queue is empty
    REQUIRE(funcQueue->size() == 0);

    // Send the message to the server
    FunctionCallClient client(LOCALHOST);
    client.shareFunctionCall(msg);

    // Check the message is on the queue
    REQUIRE(funcQueue->size() == 1);
    faabric::Message actual = funcQueue->dequeue();
    REQUIRE(actual.user() == msg.user());
    REQUIRE(actual.function() == msg.function());
    REQUIRE(actual.inputdata() == msg.inputdata());

    // Stop the server
    server.stop();
}

TEST_CASE("Test sending MPI message", "[scheduler]")
{
    cleanFaabric();

    // Start the server
    ServerContext serverContext;
    FunctionCallServer server;
    server.start();
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
    FunctionCallClient cli(LOCALHOST);
    cli.sendMPIMessage(mpiMsg);

    // Make sure the message has been put on the right queue locally
    std::shared_ptr<InMemoryMpiQueue> queue =
      localWorld.getLocalQueue(rankRemote, rankLocal);
    REQUIRE(queue->size() == 1);
    const faabric::MPIMessage& actualMessage = queue->dequeue();

    REQUIRE(actualMessage.worldid() == worldId);
    REQUIRE(actualMessage.sender() == rankRemote);

    // Stop the server
    server.stop();
}

TEST_CASE("Test sending flush message", "[scheduler]")
{
    cleanFaabric();

    // Start the server
    ServerContext serverContext;
    FunctionCallServer server;
    server.start();
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
    FunctionCallClient cli(LOCALHOST);
    cli.sendFlush();

    // Wait for thread to get flush message
    if (tA.joinable()) {
        tA.join();
    }

    if (tB.joinable()) {
        tB.join();
    }

    server.stop();

    // Check the scheduler has been flushed
    REQUIRE(sch.getFunctionRegisteredHostCount(msgA) == 0);
    REQUIRE(sch.getFunctionRegisteredHostCount(msgB) == 0);

    // Check state has been cleared
    REQUIRE(state.getKVCount() == 0);
}

TEST_CASE("Test broadcasting flush message", "[scheduler]")
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
}
