#include <catch.hpp>

#include "faabric/redis/Redis.h"
#include "faabric/scheduler/Scheduler.h"
#include "faabric/util/func.h"
#include "faabric_utils.h"

#include <faabric/scheduler/FunctionCallClient.h>
#include <faabric/scheduler/FunctionCallServer.h>
#include <faabric/scheduler/MpiWorld.h>
#include <faabric/scheduler/MpiWorldRegistry.h>
#include <faabric/util/config.h>
#include <faabric/util/network.h>

using namespace scheduler;

namespace tests {
TEST_CASE("Test sending function call", "[scheduler]")
{
    // Start the server
    ServerContext serverContext;
    FunctionCallServer server;
    server.start();
    usleep(1000 * 100);

    // Create a message
    faabric::Message msg;
    msg.set_user("demo");
    msg.set_function("echo");
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
    // Start the server
    ServerContext serverContext;
    FunctionCallServer server;
    server.start();
    usleep(1000 * 100);

    // Execute a function
    faabric::Message msg = faabric::util::messageFactory("demo", "hello");
    faabric::scheduler::Scheduler& sch = faabric::scheduler::getScheduler();
    sch.callFunction(msg);

    // Empty the queued messages
    auto bindQueue = sch.getBindQueue();
    auto functionQueue = sch.getFunctionQueue(msg);
    bindQueue->dequeue();
    functionQueue->dequeue();

    // Check the host is present before
    std::string thisHost = sch.getThisHost();
    std::string warmSetName = sch.getFunctionWarmSetName(msg);
    faabric::redis::Redis& redis = faabric::redis::Redis::getQueue();
    REQUIRE(redis.sismember(warmSetName, thisHost));

    // Background thread to get flush message
    std::thread t([&functionQueue] {
        faabric::Message msg = functionQueue->dequeue(1000);
        REQUIRE(msg.type() == faabric::Message_MessageType_FLUSH);
    });

    // Send flush message
    FunctionCallClient cli(LOCALHOST);
    cli.sendFlush();

    // Wait for thread to get flush message
    if (t.joinable()) {
        t.join();
    }

    server.stop();

    // Check host has now been removed
    REQUIRE(!redis.sismember(warmSetName, thisHost));
}
}
