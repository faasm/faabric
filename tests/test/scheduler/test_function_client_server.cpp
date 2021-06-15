#include <catch.hpp>

#include <DummyExecutor.h>
#include <DummyExecutorFactory.h>

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/ExecutorFactory.h>
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

#define TEST_TIMEOUT_MS 500

using namespace faabric::scheduler;

namespace tests {
class ClientServerFixture
  : public RedisTestFixture
  , public SchedulerTestFixture
  , public StateTestFixture
{
  protected:
    FunctionCallServer server;
    FunctionCallClient cli;
    std::shared_ptr<DummyExecutorFactory> executorFactory;

  public:
    ClientServerFixture()
      : cli(LOCALHOST)
    {
        server.start();
        usleep(1000 * TEST_TIMEOUT_MS);

        // Set up executor
        executorFactory = std::make_shared<DummyExecutorFactory>();
        setExecutorFactory(executorFactory);
    }

    ~ClientServerFixture()
    {
        cli.close();
        server.stop();
        executorFactory->reset();
    }
};

TEST_CASE_METHOD(ClientServerFixture, "Test sending MPI message", "[scheduler]")
{
    auto& sch = faabric::scheduler::getScheduler();

    // Force the scheduler to initialise a world in the remote host by setting
    // a world size bigger than the slots available locally
    int worldSize = 2;
    faabric::HostResources localResources;
    localResources.set_slots(1);
    localResources.set_usedslots(1);
    faabric::HostResources otherResources;
    otherResources.set_slots(1);

    // Set up a remote host
    std::string otherHost = LOCALHOST;
    sch.addHostToGlobalSet(otherHost);

    // Mock everything to make sure the other host has resources as well
    faabric::util::setMockMode(true);
    sch.setThisHostResources(localResources);
    faabric::scheduler::queueResourceResponse(otherHost, otherResources);

    // Create an MPI world on this host and one on a "remote" host
    const char* user = "mpi";
    const char* func = "hellompi";
    int worldId = 123;
    faabric::Message msg;
    msg.set_user(user);
    msg.set_function(func);
    msg.set_mpiworldid(worldId);
    msg.set_mpiworldsize(worldSize);
    faabric::util::messageFactory(user, func);

    scheduler::MpiWorldRegistry& registry = getMpiWorldRegistry();
    scheduler::MpiWorld& localWorld = registry.createWorld(msg, worldId);

    scheduler::MpiWorld remoteWorld;
    remoteWorld.overrideHost(otherHost);
    remoteWorld.initialiseFromMsg(msg);

    // Register a rank on each
    int rankLocal = 0;
    int rankRemote = 1;

    // Undo the mocking, so we actually send the MPI message
    faabric::util::setMockMode(false);

    // Create a message
    faabric::MPIMessage mpiMsg;
    mpiMsg.set_worldid(worldId);
    mpiMsg.set_sender(rankRemote);
    mpiMsg.set_destination(rankLocal);

    // Send the message
    cli.sendMPIMessage(std::make_shared<faabric::MPIMessage>(mpiMsg));
    usleep(1000 * TEST_TIMEOUT_MS);

    // Make sure the message has been put on the right queue locally
    std::shared_ptr<InMemoryMpiQueue> queue =
      localWorld.getLocalQueue(rankRemote, rankLocal);
    REQUIRE(queue->size() == 1);
    const std::shared_ptr<faabric::MPIMessage> actualMessage = queue->dequeue();

    REQUIRE(actualMessage->worldid() == worldId);
    REQUIRE(actualMessage->sender() == rankRemote);

    localWorld.destroy();
    remoteWorld.destroy();
    registry.clear();
}

TEST_CASE_METHOD(ClientServerFixture,
                 "Test sending flush message",
                 "[scheduler]")
{
    // Check no flushes to begin with
    REQUIRE(executorFactory->getFlushCount() == 0);

    // Set up some state
    faabric::state::State& state = faabric::state::getGlobalState();
    state.getKV("demo", "blah", 10);
    state.getKV("other", "foo", 30);

    REQUIRE(state.getKVCount() == 2);

    // Execute a couple of functions
    faabric::Message msgA = faabric::util::messageFactory("dummy", "foo");
    faabric::Message msgB = faabric::util::messageFactory("dummy", "bar");
    sch.callFunction(msgA);
    sch.callFunction(msgB);

    // Check messages passed
    std::vector<faabric::Message> msgs = sch.getRecordedMessagesAll();
    REQUIRE(msgs.size() == 2);
    REQUIRE(msgs.at(0).function() == "foo");
    REQUIRE(msgs.at(1).function() == "bar");
    sch.clearRecordedMessages();

    // Send flush message
    cli.sendFlush();
    usleep(1000 * TEST_TIMEOUT_MS);

    // Check the scheduler has been flushed
    REQUIRE(sch.getFunctionRegisteredHostCount(msgA) == 0);
    REQUIRE(sch.getFunctionRegisteredHostCount(msgB) == 0);

    // Check state has been cleared
    REQUIRE(state.getKVCount() == 0);

    // Check the flush hook has been called
    int flushCount = executorFactory->getFlushCount();
    REQUIRE(flushCount == 1);
}

TEST_CASE_METHOD(ClientServerFixture,
                 "Test broadcasting flush message",
                 "[scheduler]")
{
    faabric::util::setMockMode(true);

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
    faabric::scheduler::clearMockRequests();
}

TEST_CASE_METHOD(ClientServerFixture,
                 "Test client batch execution request",
                 "[scheduler]")
{
    // Set up a load of calls
    int nCalls = 30;
    std::shared_ptr<faabric::BatchExecuteRequest> req =
      faabric::util::batchExecFactory("foo", "bar", nCalls);

    // Make the request
    cli.executeFunctions(req);
    usleep(1000 * TEST_TIMEOUT_MS);

    // Check no other hosts have been registered
    faabric::Message m = req->messages().at(0);
    REQUIRE(sch.getFunctionRegisteredHostCount(m) == 0);

    // Check calls have been registered
    REQUIRE(sch.getRecordedMessagesLocal().size() == nCalls);
    REQUIRE(sch.getRecordedMessagesShared().empty());
}

TEST_CASE_METHOD(ClientServerFixture,
                 "Test get resources request",
                 "[scheduler]")
{
    int expectedSlots;
    int expectedUsedSlots;

    faabric::HostResources originalResources;
    originalResources.set_slots(sch.getThisHostResources().slots());

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

    // Make the request
    faabric::HostResources resResponse = cli.getResources();

    REQUIRE(resResponse.slots() == expectedSlots);
    REQUIRE(resResponse.usedslots() == expectedUsedSlots);

    // Reset the host resources
    sch.setThisHostResources(originalResources);
}

TEST_CASE_METHOD(ClientServerFixture, "Test unregister request", "[scheduler]")
{
    faabric::util::setMockMode(true);
    std::string otherHost = "other";

    faabric::HostResources originalResources;
    originalResources.set_slots(sch.getThisHostResources().slots());

    // Remove capacity from this host and add on other
    faabric::HostResources thisResources;
    faabric::HostResources otherResources;
    thisResources.set_slots(0);
    otherResources.set_slots(5);

    sch.setThisHostResources(thisResources);
    faabric::scheduler::queueResourceResponse(otherHost, otherResources);

    // Request a function and check the other host is registered
    faabric::Message msg = faabric::util::messageFactory("foo", "bar");
    sch.addHostToGlobalSet(otherHost);
    sch.callFunction(msg);

    REQUIRE(sch.getFunctionRegisteredHostCount(msg) == 1);

    faabric::util::setMockMode(false);
    faabric::scheduler::clearMockRequests();

    // Make the request with a host that's not registered
    faabric::UnregisterRequest reqA;
    reqA.set_host("foobar");
    *reqA.mutable_function() = msg;

    cli.unregister(reqA);
    REQUIRE(sch.getFunctionRegisteredHostCount(msg) == 1);

    // Make the request to unregister the actual host
    faabric::UnregisterRequest reqB;
    reqB.set_host(otherHost);
    *reqB.mutable_function() = msg;
    cli.unregister(reqB);
    usleep(1000 * TEST_TIMEOUT_MS);
    REQUIRE(sch.getFunctionRegisteredHostCount(msg) == 0);

    sch.setThisHostResources(originalResources);
    faabric::scheduler::clearMockRequests();
}
}
