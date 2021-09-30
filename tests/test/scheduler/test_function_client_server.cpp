#include <catch.hpp>
#include <faabric_utils.h>

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
#include <faabric/util/macros.h>
#include <faabric/util/network.h>
#include <faabric/util/testing.h>

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
    DistributedCoordinator& sync;

  public:
    ClientServerFixture()
      : cli(LOCALHOST)
      , sync(getDistributedCoordinator())
    {
        sync.clear();

        // Set up executor
        executorFactory = std::make_shared<DummyExecutorFactory>();
        setExecutorFactory(executorFactory);

        server.start();
    }

    ~ClientServerFixture()
    {
        sync.clear();

        server.stop();
        executorFactory->reset();
    }
};

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

    // Send flush message (which is synchronous)
    cli.sendFlush();

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

    for (const auto& m : req->messages()) {
        // This timeout can be long as it shouldn't fail
        sch.getFunctionResult(m.id(), 5 * SHORT_TEST_TIMEOUT_MS);
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

    // Check that nothing's happened
    server.setAsyncLatch();
    cli.unregister(reqA);
    server.awaitAsyncLatch();
    REQUIRE(sch.getFunctionRegisteredHostCount(msg) == 1);

    // Make the request to unregister the actual host
    faabric::UnregisterRequest reqB;
    reqB.set_host(otherHost);
    *reqB.mutable_function() = msg;

    server.setAsyncLatch();
    cli.unregister(reqB);
    server.awaitAsyncLatch();

    REQUIRE(sch.getFunctionRegisteredHostCount(msg) == 0);

    sch.setThisHostResources(originalResources);
    faabric::scheduler::clearMockRequests();
}

TEST_CASE_METHOD(ClientServerFixture,
                 "Test distributed lock/ unlock",
                 "[scheduler][sync]")
{
    faabric::Message msg = faabric::util::messageFactory("foo", "bar");
    msg.set_groupid(123);

    REQUIRE(!sync.isLocalLocked(msg));

    cli.coordinationLock(msg);

    REQUIRE(sync.isLocalLocked(msg));

    cli.coordinationUnlock(msg);

    REQUIRE(!sync.isLocalLocked(msg));
}

TEST_CASE_METHOD(ClientServerFixture,
                 "Test distributed notify",
                 "[scheduler][sync]")
{
    faabric::Message msg = faabric::util::messageFactory("foo", "bar");
    msg.set_groupsize(5);
    msg.set_groupid(123);

    REQUIRE(sync.getNotifyCount(msg) == 0);

    cli.coordinationNotify(msg);

    REQUIRE(sync.getNotifyCount(msg) == 1);

    cli.coordinationNotify(msg);
    cli.coordinationNotify(msg);

    REQUIRE(sync.getNotifyCount(msg) == 3);
}

TEST_CASE_METHOD(ClientServerFixture,
                 "Test distributed barrier",
                 "[scheduler][sync]")
{
    faabric::Message msg = faabric::util::messageFactory("foo", "bar");
    msg.set_groupid(123);
    msg.set_groupsize(2);

    REQUIRE(sync.getNotifyCount(msg) == 0);

    std::thread t([&msg] {
        FunctionCallClient cli(LOCALHOST);
        cli.coordinationBarrier(msg);
    });

    // Wait on the barrier in this thread
    sync.barrier(msg);

    // Let the thread in the background also wait on the barrier
    if (t.joinable()) {
        t.join();
    }

    // Here we know the test has passed
}
}
