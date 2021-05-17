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

static void tearDown(FunctionCallClient& cli, FunctionCallServer& server)
{
    cli.close();
    server.stop();
    faabric::scheduler::getScheduler().reset();
}

namespace tests {

TEST_CASE("Test sending MPI message", "[scheduler]")
{
    cleanFaabric();

    // Start the server
    FunctionCallServer server;
    server.start();
    usleep(1000 * 100);

    // Create an MPI world on this host and one on a "remote" host
    std::string otherHost = "192.168.9.2";

    const char* user = "mpi";
    const char* func = "hellompi";
    int worldId = 123;
    faabric::Message msg;
    msg.set_user(user);
    msg.set_function(func);
    msg.set_mpiworldid(worldId);
    msg.set_mpiworldsize(2);
    faabric::util::messageFactory(user, func);

    scheduler::MpiWorldRegistry& registry = getMpiWorldRegistry();
    scheduler::MpiWorld& localWorld =
      registry.createWorld(msg, worldId, LOCALHOST);

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
    cli.sendMPIMessage(std::make_shared<faabric::MPIMessage>(mpiMsg));
    usleep(1000 * 100);

    // Make sure the message has been put on the right queue locally
    std::shared_ptr<InMemoryMpiQueue> queue =
      localWorld.getLocalQueue(rankRemote, rankLocal);
    REQUIRE(queue->size() == 1);
    const std::shared_ptr<faabric::MPIMessage> actualMessage = queue->dequeue();

    REQUIRE(actualMessage->worldid() == worldId);
    REQUIRE(actualMessage->sender() == rankRemote);

    localWorld.destroy();
    remoteWorld.destroy();

    tearDown(cli, server);
}

TEST_CASE("Test sending flush message", "[scheduler]")
{
    cleanFaabric();

    // Start the server
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

    // Check messages passed
    std::vector<faabric::Message> msgs = sch.getRecordedMessagesAll();
    REQUIRE(msgs.size() == 2);
    REQUIRE(msgs.at(0).function() == "foo");
    REQUIRE(msgs.at(1).function() == "bar");
    sch.clearRecordedMessages();

    // Send flush message
    FunctionCallClient cli(LOCALHOST);
    cli.sendFlush();
    usleep(1000 * 100);

    // Check the scheduler has been flushed
    REQUIRE(sch.getFunctionRegisteredHostCount(msgA) == 0);
    REQUIRE(sch.getFunctionRegisteredHostCount(msgB) == 0);

    // Check state has been cleared
    REQUIRE(state.getKVCount() == 0);

    tearDown(cli, server);
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

    faabric::scheduler::getScheduler().reset();
}

TEST_CASE("Test client batch execution request", "[scheduler]")
{
    cleanFaabric();

    // Start the server
    FunctionCallServer server;
    server.start();
    usleep(1000 * 100);

    // Set up a load of calls
    int nCalls = 30;
    std::shared_ptr<faabric::BatchExecuteRequest> req =
      faabric::util::batchExecFactory("foo", "bar", nCalls);

    // Make the request
    FunctionCallClient cli(LOCALHOST);
    cli.executeFunctions(req);
    usleep(1000 * 300);

    faabric::scheduler::Scheduler& sch = faabric::scheduler::getScheduler();

    // Check no other hosts have been registered
    faabric::Message m = req->messages().at(0);
    REQUIRE(sch.getFunctionRegisteredHostCount(m) == 0);

    // Check calls have been registered
    REQUIRE(sch.getRecordedMessagesLocal().size() == nCalls);
    REQUIRE(sch.getRecordedMessagesShared().empty());

    tearDown(cli, server);
}

TEST_CASE("Test get resources request", "[scheduler]")
{
    cleanFaabric();

    faabric::scheduler::Scheduler& sch = faabric::scheduler::getScheduler();

    int expectedSlots;
    int expectedUsedSlots;

    SECTION("Override resources")
    {
        faabric::HostResources res;

        expectedSlots = 10;
        expectedUsedSlots = 15;

        res.set_slots(expectedSlots);
        res.set_usedslots(expectedUsedSlots);

        sch.setThisHostResources(res);
    }
    SECTION("Default resources")
    {
        expectedSlots = sch.getThisHostResources().slots();
        expectedUsedSlots = 0;
    }

    // Start the server
    FunctionCallServer server;
    server.start();
    usleep(1000 * 100);

    // Make the request
    FunctionCallClient cli(LOCALHOST);
    faabric::HostResources resResponse = cli.getResources();

    REQUIRE(resResponse.slots() == expectedSlots);
    REQUIRE(resResponse.usedslots() == expectedUsedSlots);

    tearDown(cli, server);
}

TEST_CASE("Test unregister request", "[scheduler]")
{
    cleanFaabric();

    faabric::util::setMockMode(true);
    std::string otherHost = "other";

    // Remove capacity from this host and add on other
    faabric::HostResources thisResources;
    faabric::HostResources otherResources;
    thisResources.set_slots(0);
    otherResources.set_slots(5);

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
    FunctionCallServer server;
    server.start();
    usleep(1000 * 100);

    // Make the request with a host that's not registered
    faabric::UnregisterRequest reqA;
    reqA.set_host("foobar");
    *reqA.mutable_function() = msg;

    FunctionCallClient cli(LOCALHOST);
    cli.unregister(reqA);
    REQUIRE(sch.getFunctionRegisteredHostCount(msg) == 1);

    // Make the request to unregister the actual host
    faabric::UnregisterRequest reqB;
    reqB.set_host(otherHost);
    *reqB.mutable_function() = msg;
    cli.unregister(reqB);
    usleep(1000 * 100);
    REQUIRE(sch.getFunctionRegisteredHostCount(msg) == 0);

    tearDown(cli, server);
}

TEST_CASE("Test set thread result", "[scheduler]")
{
    cleanFaabric();

    // Register threads on this host
    int threadIdA = 123;
    int threadIdB = 345;
    int returnValueA = 88;
    int returnValueB = 99;
    faabric::scheduler::Scheduler& sch = faabric::scheduler::getScheduler();
    sch.registerThread(threadIdA);
    sch.registerThread(threadIdB);

    // Start the server
    FunctionCallServer server;
    server.start();
    usleep(1000 * 100);

    // Make the request
    faabric::ThreadResultRequest reqA;
    faabric::ThreadResultRequest reqB;

    reqA.set_messageid(threadIdA);
    reqA.set_returnvalue(returnValueA);

    reqB.set_messageid(threadIdB);
    reqB.set_returnvalue(returnValueB);

    // Set up two threads to await the results
    std::thread tA([threadIdA, returnValueA] {
        faabric::scheduler::Scheduler& sch = faabric::scheduler::getScheduler();
        int32_t r = sch.awaitThreadResult(threadIdA);
        REQUIRE(r == returnValueA);
    });

    std::thread tB([threadIdB, returnValueB] {
        faabric::scheduler::Scheduler& sch = faabric::scheduler::getScheduler();
        int32_t r = sch.awaitThreadResult(threadIdB);
        REQUIRE(r == returnValueB);
    });

    FunctionCallClient cli(LOCALHOST);
    cli.setThreadResult(reqA);
    cli.setThreadResult(reqB);

    if (tA.joinable()) {
        tA.join();
    }

    if (tB.joinable()) {
        tB.join();
    }

    tearDown(cli, server);
}
}
