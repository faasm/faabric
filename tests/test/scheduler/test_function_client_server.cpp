#include <catch2/catch.hpp>

#include "faabric_utils.h"
#include "fixtures.h"

#include <DummyExecutor.h>
#include <DummyExecutorFactory.h>

#include <faabric/mpi/MpiWorld.h>
#include <faabric/mpi/MpiWorldRegistry.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/ExecutorFactory.h>
#include <faabric/scheduler/FunctionCallClient.h>
#include <faabric/scheduler/FunctionCallServer.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/config.h>
#include <faabric/util/environment.h>
#include <faabric/util/func.h>
#include <faabric/util/macros.h>
#include <faabric/util/network.h>
#include <faabric/util/testing.h>

using namespace faabric::scheduler;

namespace tests {
class ClientServerFixture
  : public RedisTestFixture
  , public StateTestFixture
  , public SchedulerTestFixture
  , public PointToPointClientServerFixture
  , public ConfTestFixture
{
  protected:
    FunctionCallServer server;
    FunctionCallClient cli;

    std::shared_ptr<DummyExecutorFactory> executorFactory;

    int groupId = 123;
    int groupSize = 2;

  public:
    ClientServerFixture()
      : cli(LOCALHOST)
    {
        // Set up executor
        executorFactory = std::make_shared<DummyExecutorFactory>();
        setExecutorFactory(executorFactory);

        server.start();
    }

    ~ClientServerFixture()
    {
        server.stop();
        executorFactory->reset();
    }
};

TEST_CASE_METHOD(ConfTestFixture,
                 "Test setting function call server threads",
                 "[scheduler]")
{
    conf.functionServerThreads = 6;

    faabric::scheduler::FunctionCallServer server;

    REQUIRE(server.getNThreads() == 6);
}

TEST_CASE_METHOD(ClientServerFixture,
                 "Test flushing executors with function call",
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
    auto reqA = faabric::util::batchExecFactory("dummy", "foo", 1);
    auto msgA = reqA->messages().at(0);
    auto reqB = faabric::util::batchExecFactory("dummy", "bar", 1);
    auto msgB = reqB->messages().at(0);
    sch.callFunctions(reqA);
    sch.callFunctions(reqB);

    // Wait for functions to finish
    sch.getFunctionResult(msgA, 2000);
    sch.getFunctionResult(msgB, 2000);

    // Check messages passed
    std::vector<faabric::Message> msgs = sch.getRecordedMessagesAll();
    REQUIRE(msgs.size() == 2);
    REQUIRE(msgs.at(0).function() == "foo");
    REQUIRE(msgs.at(1).function() == "bar");
    sch.clearRecordedMessages();

    // Check executors present
    REQUIRE(sch.getFunctionExecutorCount(msgA) == 1);
    REQUIRE(sch.getFunctionExecutorCount(msgB) == 1);

    // Check executors present
    REQUIRE(sch.getFunctionExecutorCount(msgA) == 1);
    REQUIRE(sch.getFunctionExecutorCount(msgB) == 1);

    // Send flush message (which is synchronous)
    cli.sendFlush();

    // Check the scheduler has been flushed
    REQUIRE(sch.getFunctionExecutorCount(msgA) == 0);
    REQUIRE(sch.getFunctionExecutorCount(msgB) == 0);

    // Check state has been cleared
    REQUIRE(state.getKVCount() == 0);

    // Check the flush hook has been called
    int flushCount = executorFactory->getFlushCount();
    REQUIRE(flushCount == 1);
}

TEST_CASE_METHOD(ClientServerFixture,
                 "Test client batch execution request",
                 "[scheduler]")
{
    // Make sure we have enough space in the scheduler
    auto thisHostResources = std::make_shared<faabric::HostResources>();
    thisHostResources->set_slots(40);
    thisHostResources->set_usedslots(0);
    sch.addHostToGlobalSet(conf.endpointHost, thisHostResources);

    // Set up a load of calls
    int nCalls = 30;
    std::shared_ptr<faabric::BatchExecuteRequest> req =
      faabric::util::batchExecFactory("foo", "bar", nCalls);

    // Make the request
    // cli.executeFunctions(req);
    sch.callFunctions(req);

    for (const auto& m : req->messages()) {
        // This timeout can be long as it shouldn't fail
        sch.getFunctionResult(m, 5 * SHORT_TEST_TIMEOUT_MS);
    }

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
    // TODO: fix me
    // originalResources.set_slots(sch.getThisHostResources().slots());

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
        // TODO: fix me
        // expectedSlots = sch.getThisHostResources().slots();
        expectedSlots = 0;
        expectedUsedSlots = 0;
    }

    // Make the request
    faabric::HostResources resResponse = cli.getResources();

    REQUIRE(resResponse.slots() == expectedSlots);
    REQUIRE(resResponse.usedslots() == expectedUsedSlots);

    // Reset the host resources
    sch.setThisHostResources(originalResources);
}

/* TODO: do we want to keep the unregister request?
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
    auto req = faabric::util::batchExecFactory("foo", "bar", 1);
    auto msg = req->messages().at(0);
    sch.addHostToGlobalSet(otherHost);
    sch.callFunctions(req);

    REQUIRE(sch.getFunctionRegisteredHostCount(msg) == 1);

    faabric::util::setMockMode(false);
    faabric::scheduler::clearMockRequests();

    // Make the request with a host that's not registered
    faabric::UnregisterRequest reqA;
    reqA.set_host("foobar");
    reqA.set_user(msg.user());
    reqA.set_function(msg.function());

    // Check that nothing's happened
    server.setRequestLatch();
    cli.unregister(reqA);
    server.awaitRequestLatch();
    REQUIRE(sch.getFunctionRegisteredHostCount(msg) == 1);

    // Make the request to unregister the actual host
    faabric::UnregisterRequest reqB;
    reqB.set_host(otherHost);
    reqB.set_user(msg.user());
    reqB.set_function(msg.function());

    server.setRequestLatch();
    cli.unregister(reqB);
    server.awaitRequestLatch();

    REQUIRE(sch.getFunctionRegisteredHostCount(msg) == 0);

    sch.setThisHostResources(originalResources);
    faabric::scheduler::clearMockRequests();
}
*/
}
