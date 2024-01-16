#include <catch2/catch.hpp>

#include "faabric_utils.h"
#include "fixtures.h"

#include <DummyExecutor.h>
#include <DummyExecutorFactory.h>

#include <faabric/executor/ExecutorFactory.h>
#include <faabric/mpi/MpiWorld.h>
#include <faabric/mpi/MpiWorldRegistry.h>
#include <faabric/proto/faabric.pb.h>
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
class FunctionClientServerTestFixture
  : public FunctionCallClientServerFixture
  , public SchedulerFixture
{
  public:
    FunctionClientServerTestFixture()
    {
        executorFactory =
          std::make_shared<faabric::executor::DummyExecutorFactory>();
        setExecutorFactory(executorFactory);
    }

    ~FunctionClientServerTestFixture() { executorFactory->reset(); }

  protected:
    std::shared_ptr<faabric::executor::DummyExecutorFactory> executorFactory;
};

TEST_CASE_METHOD(ConfFixture,
                 "Test setting function call server threads",
                 "[scheduler]")
{
    conf.functionServerThreads = 6;

    faabric::scheduler::FunctionCallServer server;

    REQUIRE(server.getNThreads() == 6);
}

TEST_CASE_METHOD(FunctionClientServerTestFixture,
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
    auto reqA = faabric::util::batchExecFactory("dummy", "foo", 1);
    auto msgA = reqA->messages(0);
    auto reqB = faabric::util::batchExecFactory("dummy", "bar", 1);
    auto msgB = reqB->messages(0);
    plannerCli.callFunctions(reqA);
    plannerCli.callFunctions(reqB);

    // Wait for functions to finish
    plannerCli.getMessageResult(msgA, 2000);
    plannerCli.getMessageResult(msgB, 2000);

    // Check messages passed
    std::vector<faabric::Message> msgs = sch.getRecordedMessages();
    REQUIRE(msgs.size() == 2);
    REQUIRE(msgs.at(0).function() == "foo");
    REQUIRE(msgs.at(1).function() == "bar");
    sch.clearRecordedMessages();

    // Check executors present
    REQUIRE(sch.getFunctionExecutorCount(msgA) == 1);
    REQUIRE(sch.getFunctionExecutorCount(msgB) == 1);

    // Send flush message (which is synchronous)
    functionCallClient.sendFlush();

    // Check the scheduler has been flushed
    REQUIRE(sch.getFunctionExecutorCount(msgA) == 0);
    REQUIRE(sch.getFunctionExecutorCount(msgB) == 0);

    // Check state has been cleared
    REQUIRE(state.getKVCount() == 0);

    // Check the flush hook has been called
    int flushCount = executorFactory->getFlushCount();
    REQUIRE(flushCount == 1);
}

TEST_CASE_METHOD(FunctionClientServerTestFixture,
                 "Test client batch execution request",
                 "[scheduler]")
{
    // Set up a load of calls
    int nCalls = 30;
    std::shared_ptr<faabric::BatchExecuteRequest> req =
      faabric::util::batchExecFactory("foo", "bar", nCalls);
    for (int i = 0; i < req->messages_size(); i++) {
        req->mutable_messages(i)->set_executedhost(
          faabric::util::getSystemConfig().endpointHost);
    }

    // Set resources (used and normal, as we bypass the planner for scheduling)
    HostResources localHost;
    localHost.set_slots(nCalls);
    localHost.set_usedslots(nCalls);
    sch.setThisHostResources(localHost);

    // Make the request
    functionCallClient.executeFunctions(req);

    for (const auto& m : req->messages()) {
        // This timeout can be long as it shouldn't fail
        plannerCli.getMessageResult(m, 5 * SHORT_TEST_TIMEOUT_MS);
    }

    // Check calls have been registered
    REQUIRE(sch.getRecordedMessages().size() == nCalls);
}

TEST_CASE_METHOD(FunctionClientServerTestFixture,
                 "Test setting a message result with the function call client",
                 "[scheduler]")
{
    auto msg = faabric::util::messageFactory("foo", "bar");
    auto msgPtr = std::make_shared<faabric::Message>(msg);

    // Setting a message result with the function call client is used when the
    // planner is notifying that a message result is ready

    int expectedReturnCode = 1337;
    int actualReturnCode;
    // This thread will block waiting for another thread to set the message
    // result
    std::jthread waiterThread{ [&] {
        auto resultMsg = plannerCli.getMessageResult(msg, 2000);
        actualReturnCode = resultMsg.returnvalue();
    } };

    SLEEP_MS(500);
    msgPtr->set_returnvalue(expectedReturnCode);
    functionCallClient.setMessageResult(msgPtr);
    waiterThread.join();

    REQUIRE(expectedReturnCode == actualReturnCode);
}
}
