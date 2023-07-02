#include <catch2/catch.hpp>

#include "DummyExecutorFactory.h"
#include "faabric_utils.h"
#include "fixtures.h"

#include <faabric/proto/faabric.pb.h>
#include <faabric/redis/Redis.h>
#include <faabric/scheduler/ExecutorFactory.h>
#include <faabric/scheduler/FunctionCallClient.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/snapshot/SnapshotClient.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/transport/PointToPointBroker.h>
#include <faabric/transport/PointToPointClient.h>
#include <faabric/util/ExecGraph.h>
#include <faabric/util/environment.h>
#include <faabric/util/func.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>
#include <faabric/util/scheduling.h>
#include <faabric/util/snapshot.h>
#include <faabric/util/testing.h>

using namespace faabric::scheduler;

namespace tests {

class SlowExecutor final : public Executor
{
  public:
    SlowExecutor(faabric::Message& msg)
      : Executor(msg)
    {
        setUpDummyMemory(dummyMemorySize);
    }

    ~SlowExecutor() {}

    int32_t executeTask(
      int threadPoolIdx,
      int msgIdx,
      std::shared_ptr<faabric::BatchExecuteRequest> req) override
    {
        SPDLOG_DEBUG("Slow executor executing task{}",
                     req->mutable_messages()->at(msgIdx).id());

        SLEEP_MS(SHORT_TEST_TIMEOUT_MS);
        return 0;
    }

    std::span<uint8_t> getMemoryView() override
    {
        return { dummyMemory.get(), dummyMemorySize };
    }

    void setUpDummyMemory(size_t memSize)
    {
        SPDLOG_DEBUG("Slow test executor initialising memory size {}", memSize);
        dummyMemory = faabric::util::allocatePrivateMemory(memSize);
        dummyMemorySize = memSize;
    }

  private:
    faabric::util::MemoryRegion dummyMemory = nullptr;
    size_t dummyMemorySize = 2 * faabric::util::HOST_PAGE_SIZE;
};

class SlowExecutorFactory : public ExecutorFactory
{
  protected:
    std::shared_ptr<Executor> createExecutor(faabric::Message& msg) override
    {
        return std::make_shared<SlowExecutor>(msg);
    }
};

class SlowExecutorTestFixture
  : public FunctionCallClientServerFixture
  , public SchedulerFixture
  , public SnapshotRegistryFixture
  , public ConfFixture
{
  public:
    SlowExecutorTestFixture()
    {
        std::shared_ptr<ExecutorFactory> fac =
          std::make_shared<SlowExecutorFactory>();
        setExecutorFactory(fac);
    };

    ~SlowExecutorTestFixture()
    {
        std::shared_ptr<DummyExecutorFactory> fac =
          std::make_shared<DummyExecutorFactory>();
        setExecutorFactory(fac);
    };
};

class DummyExecutorTestFixture
  : public FunctionCallClientServerFixture
  , public PointToPointBrokerFixture
  , public SchedulerFixture
  , public ConfFixture
{
  public:
    DummyExecutorTestFixture()
    {
        std::shared_ptr<ExecutorFactory> fac =
          std::make_shared<DummyExecutorFactory>();
        setExecutorFactory(fac);
    };

    ~DummyExecutorTestFixture()
    {
        std::shared_ptr<DummyExecutorFactory> fac =
          std::make_shared<DummyExecutorFactory>();
        setExecutorFactory(fac);
    };
};

TEST_CASE_METHOD(SlowExecutorTestFixture,
                 "Test scheduler clear-up",
                 "[scheduler]")
{
    faabric::util::setMockMode(true);

    std::string thisHost = conf.endpointHost;
    std::string otherHost = "other";
    std::set<std::string> expectedHosts = { otherHost };

    sch.addHostToGlobalSet(otherHost);

    // Set resources
    int nCores = 5;
    faabric::HostResources res;
    res.set_slots(nCores);
    sch.setThisHostResources(res);

    // Set resources for other host too
    faabric::scheduler::queueResourceResponse(otherHost, res);

    // Set request
    int nCalls = nCores + 1;
    auto req = faabric::util::batchExecFactory("blah", "foo", nCalls);
    auto msg = req->messages(0);

    // Initial checks
    REQUIRE(sch.getFunctionExecutorCount(msg) == 0);
    REQUIRE(sch.getFunctionRegisteredHostCount(msg) == 0);
    REQUIRE(sch.getFunctionRegisteredHosts(msg.user(), msg.function()).empty());

    faabric::HostResources resCheck = sch.getThisHostResources();
    REQUIRE(resCheck.slots() == nCores);
    REQUIRE(resCheck.usedslots() == 0);
    REQUIRE(sch.getThisHostResources().slots() == nCores);

    // Make calls with one extra that should be sent to the other host
    sch.callFunctions(req);

    REQUIRE(sch.getFunctionExecutorCount(msg) == nCores);
    REQUIRE(sch.getFunctionRegisteredHostCount(msg) == 1);
    REQUIRE(sch.getFunctionRegisteredHosts(msg.user(), msg.function()) ==
            expectedHosts);

    resCheck = sch.getThisHostResources();
    REQUIRE(resCheck.slots() == nCores);
    REQUIRE(resCheck.usedslots() == nCores);

    sch.reset();

    // Check scheduler has been cleared
    REQUIRE(sch.getFunctionExecutorCount(msg) == 0);
    REQUIRE(sch.getFunctionRegisteredHostCount(msg) == 0);
    REQUIRE(sch.getFunctionRegisteredHosts(msg.user(), msg.function()).empty());

    resCheck = sch.getThisHostResources();
    int actualCores = faabric::util::getUsableCores();
    REQUIRE(resCheck.slots() == actualCores);
    REQUIRE(resCheck.usedslots() == 0);
}

TEST_CASE_METHOD(SlowExecutorTestFixture,
                 "Test scheduler available hosts",
                 "[scheduler]")
{
    // Set up some available hosts
    std::string thisHost = faabric::util::getSystemConfig().endpointHost;
    std::string hostA = "hostA";
    std::string hostB = "hostB";
    std::string hostC = "hostC";

    sch.addHostToGlobalSet(hostA);
    sch.addHostToGlobalSet(hostB);
    sch.addHostToGlobalSet(hostC);

    std::set<std::string> expectedHosts = { thisHost, hostA, hostB, hostC };
    std::set<std::string> actualHosts = sch.getAvailableHosts();

    REQUIRE(actualHosts == expectedHosts);

    sch.removeHostFromGlobalSet(hostB);
    sch.removeHostFromGlobalSet(hostC);

    expectedHosts = { thisHost, hostA };
    actualHosts = sch.getAvailableHosts();

    REQUIRE(actualHosts == expectedHosts);
}

TEST_CASE_METHOD(SlowExecutorTestFixture,
                 "Test batch scheduling",
                 "[scheduler]")
{
    std::string expectedSnapshot;
    faabric::BatchExecuteRequest::BatchExecuteType execMode;
    int32_t expectedSubType;
    std::string expectedContextData;

    int thisCores = 5;
    faabric::util::SystemConfig& conf = faabric::util::getSystemConfig();
    conf.overrideCpuCount = thisCores;

    int nCallsOne = 10;
    int nCallsTwo = 20;

    std::shared_ptr<faabric::BatchExecuteRequest> reqOne =
      faabric::util::batchExecFactory("foo", "bar", nCallsOne);
    const faabric::Message firstMsg = reqOne->messages().at(0);
    int appId = firstMsg.appid();

    size_t snapSize = 2 * faabric::util::HOST_PAGE_SIZE;
    auto snap = std::make_shared<faabric::util::SnapshotData>(snapSize);

    SECTION("Threads")
    {
        execMode = faabric::BatchExecuteRequest::THREADS;
        expectedSnapshot = faabric::util::getMainThreadSnapshotKey(firstMsg);

        expectedSubType = 123;
        expectedContextData = "thread context";
    }

    SECTION("Processes")
    {
        execMode = faabric::BatchExecuteRequest::PROCESSES;
        expectedSnapshot = "procSnap";

        expectedSubType = 345;
        expectedContextData = "proc context";
    }

    SECTION("Functions")
    {
        execMode = faabric::BatchExecuteRequest::FUNCTIONS;
        expectedSnapshot = "";
    }

    // Set up the snapshot
    if (!expectedSnapshot.empty()) {
        reg.registerSnapshot(expectedSnapshot, snap);
    }

    bool isThreads = execMode == faabric::BatchExecuteRequest::THREADS;

    // Mock everything
    faabric::util::setMockMode(true);

    std::string thisHost = conf.endpointHost;

    // Set up another host
    std::string otherHost = "beta";
    sch.addHostToGlobalSet(otherHost);

    int otherCores = 15;
    int nCallsOffloadedOne = nCallsOne - thisCores;

    faabric::HostResources thisResources;
    thisResources.set_slots(thisCores);

    faabric::HostResources otherResources;
    otherResources.set_slots(otherCores);

    // Prepare resource response for other host
    sch.setThisHostResources(thisResources);
    faabric::scheduler::queueResourceResponse(otherHost, otherResources);

    // Set up the messages
    std::vector<int> reqOneMsgIds;
    faabric::util::SchedulingDecision expectedDecisionOne(firstMsg.appid(),
                                                          firstMsg.groupid());
    for (int i = 0; i < nCallsOne; i++) {
        // Set snapshot key
        faabric::Message& msg = reqOne->mutable_messages()->at(i);

        if (!isThreads) {
            msg.set_snapshotkey(expectedSnapshot);
        }

        // Set app index
        msg.set_appidx(i);

        // Expect this host to handle up to its number of cores
        std::string host = i < thisCores ? thisHost : otherHost;
        expectedDecisionOne.addMessage(host, msg);

        reqOneMsgIds.push_back(msg.id());
    }

    // Schedule the functions
    reqOne->set_type(execMode);
    reqOne->set_subtype(expectedSubType);
    reqOne->set_contextdata(expectedContextData);

    faabric::util::SchedulingDecision actualDecisionOne =
      sch.callFunctions(reqOne);

    // Check decision is as expected
    checkSchedulingDecisionEquality(actualDecisionOne, expectedDecisionOne);

    // Await the results
    for (int i = 0; i < thisCores; i++) {
        if (isThreads) {
            sch.awaitThreadResult(reqOneMsgIds.at(i));
        } else {
            sch.getFunctionResult(appId, reqOneMsgIds.at(i), 10000);
        }
    }

    // Check resource requests have been made to other host
    auto resRequestsOne = faabric::scheduler::getResourceRequests();
    REQUIRE(resRequestsOne.size() == 1);
    REQUIRE(resRequestsOne.at(0).first == otherHost);

    // Check snapshots have been pushed
    auto snapshotPushes = faabric::snapshot::getSnapshotPushes();
    if (expectedSnapshot.empty()) {
        REQUIRE(snapshotPushes.empty());
    } else {
        REQUIRE(snapshotPushes.size() == 1);

        auto snapshot = reg.getSnapshot(expectedSnapshot);

        auto pushedSnapshot = snapshotPushes.at(0);
        REQUIRE(pushedSnapshot.first == otherHost);
        REQUIRE(pushedSnapshot.second->getSize() == snapshot->getSize());
        REQUIRE(pushedSnapshot.second->getDataPtr() == snapshot->getDataPtr());
    }

    // Check the executor counts on this host
    faabric::Message m = reqOne->messages().at(0);
    faabric::HostResources res = sch.getThisHostResources();
    if (isThreads) {
        // For threads we expect only one executor
        REQUIRE(sch.getFunctionExecutorCount(m) == 1);
    } else {
        // For functions we expect one per core
        REQUIRE(sch.getFunctionExecutorCount(m) == thisCores);
    }

    REQUIRE(res.slots() == thisCores);
    REQUIRE(res.usedslots() == 0);

    // Check the number of messages executed locally and remotely
    REQUIRE(sch.getRecordedMessagesLocal().size() == thisCores);
    REQUIRE(sch.getRecordedMessagesShared().size() == nCallsOffloadedOne);

    // Check the message is dispatched to the other host
    auto batchRequestsOne = faabric::scheduler::getBatchRequests();
    REQUIRE(batchRequestsOne.size() == 1);

    auto batchRequestOne = batchRequestsOne.at(0);
    REQUIRE(batchRequestOne.first == otherHost);
    REQUIRE(batchRequestOne.second->messages_size() == nCallsOffloadedOne);
    REQUIRE(batchRequestOne.second->type() == execMode);
    REQUIRE(batchRequestOne.second->subtype() == expectedSubType);
    REQUIRE(batchRequestOne.second->contextdata() == expectedContextData);

    // Clear mocks
    faabric::scheduler::clearMockRequests();

    // Set up resource response again
    faabric::scheduler::queueResourceResponse(otherHost, otherResources);

    // Now schedule a second batch and check the decision
    std::shared_ptr<faabric::BatchExecuteRequest> reqTwo =
      faabric::util::batchExecFactory("foo", "bar", nCallsTwo);

    std::vector<int> reqTwoMsgIds;
    const faabric::Message& firstMsg2 = reqTwo->messages().at(0);
    faabric::util::SchedulingDecision expectedDecisionTwo(appId,
                                                          firstMsg2.groupid());
    for (int i = 0; i < nCallsTwo; i++) {
        faabric::Message& msg = reqTwo->mutable_messages()->at(i);

        msg.set_appid(appId);
        msg.set_appidx(i);

        if (!isThreads) {
            msg.set_snapshotkey(expectedSnapshot);
        }

        std::string host = i < thisCores ? thisHost : otherHost;
        expectedDecisionTwo.addMessage(host, msg);

        reqTwoMsgIds.push_back(msg.id());
    }

    // Create the batch request
    reqTwo->set_type(execMode);

    // Schedule the functions
    faabric::util::SchedulingDecision actualDecisionTwo =
      sch.callFunctions(reqTwo);

    // Check scheduling decision
    checkSchedulingDecisionEquality(actualDecisionTwo, expectedDecisionTwo);

    // Await the results
    for (int i = 0; i < thisCores; i++) {
        if (isThreads) {
            sch.awaitThreadResult(reqTwoMsgIds.at(i));
        } else {
            sch.getFunctionResult(appId, reqTwoMsgIds.at(i), 10000);
        }
    }

    // Check resource request made again
    auto resRequestsTwo = faabric::scheduler::getResourceRequests();
    REQUIRE(resRequestsTwo.size() == 1);
    REQUIRE(resRequestsTwo.at(0).first == otherHost);

    // Check no other functions have been scheduled on this host
    REQUIRE(sch.getRecordedMessagesLocal().size() == (2 * thisCores));
    REQUIRE(sch.getRecordedMessagesShared().size() ==
            (nCallsOne + nCallsTwo) - (2 * thisCores));

    if (isThreads) {
        REQUIRE(sch.getFunctionExecutorCount(m) == 1);
    } else {
        REQUIRE(sch.getFunctionExecutorCount(m) == thisCores);
    }

    // Check the second message is dispatched to the other host
    auto batchRequestsTwo = faabric::scheduler::getBatchRequests();
    REQUIRE(batchRequestsTwo.size() == 1);
    auto pTwo = batchRequestsTwo.at(0);
    REQUIRE(pTwo.first == otherHost);

    // Check the request to the other host
    REQUIRE(pTwo.second->messages_size() == nCallsTwo - thisCores);
}

TEST_CASE_METHOD(SlowExecutorTestFixture,
                 "Test overloaded scheduler",
                 "[scheduler]")
{
    faabric::util::SystemConfig& conf = faabric::util::getSystemConfig();
    conf.overrideCpuCount = 5;

    faabric::util::setMockMode(true);

    faabric::BatchExecuteRequest::BatchExecuteType execMode;
    std::string expectedSnapshot;

    // Submit more calls than we have capacity for
    int nCalls = 10;
    std::shared_ptr<faabric::BatchExecuteRequest> req =
      faabric::util::batchExecFactory("foo", "bar", nCalls);

    SECTION("Threads")
    {
        execMode = faabric::BatchExecuteRequest::THREADS;
        expectedSnapshot =
          faabric::util::getMainThreadSnapshotKey(req->messages().at(0));
    }

    SECTION("Processes")
    {
        execMode = faabric::BatchExecuteRequest::PROCESSES;
        expectedSnapshot = "procSnap";
    }

    SECTION("Functions") { execMode = faabric::BatchExecuteRequest::FUNCTIONS; }

    size_t snapSize = 1234;
    if (!expectedSnapshot.empty()) {
        auto snap = std::make_shared<faabric::util::SnapshotData>(snapSize);
        reg.registerSnapshot(expectedSnapshot, snap);
    }

    // Set up this host with very low resources
    std::string thisHost = sch.getThisHost();
    int nCores = 1;
    faabric::HostResources res;
    res.set_slots(nCores);
    sch.setThisHostResources(res);

    // Set up another host with insufficient resources
    std::string otherHost = "other";
    sch.addHostToGlobalSet(otherHost);
    faabric::HostResources resOther;
    resOther.set_slots(2);
    faabric::scheduler::queueResourceResponse(otherHost, resOther);

    // Make the request
    req->set_type(execMode);
    const faabric::Message firstMsg = req->messages().at(0);
    faabric::util::SchedulingDecision expectedDecision(firstMsg.appid(),
                                                       firstMsg.groupid());
    std::vector<faabric::Message> msgToWait;
    for (int i = 0; i < nCalls; i++) {
        faabric::Message& msg = req->mutable_messages()->at(i);

        if (req->type() != faabric::BatchExecuteRequest::THREADS) {
            msg.set_snapshotkey(expectedSnapshot);
        }

        if (i == 1 || i == 2) {
            expectedDecision.addMessage(otherHost, msg);
        } else {
            msgToWait.emplace_back(msg);
            expectedDecision.addMessage(thisHost, msg);
        }
    }

    // Submit the request
    faabric::util::SchedulingDecision decision = sch.callFunctions(req);
    checkSchedulingDecisionEquality(decision, expectedDecision);

    // Check status of local queueing
    int expectedLocalCalls = nCalls - 2;
    int expectedExecutors;
    if (execMode == faabric::BatchExecuteRequest::THREADS) {
        expectedExecutors = 1;
    } else {
        expectedExecutors = expectedLocalCalls;
    }

    REQUIRE(sch.getFunctionExecutorCount(firstMsg) == expectedExecutors);

    // Await results
    for (const auto& msg : msgToWait) {
        if (execMode == faabric::BatchExecuteRequest::THREADS) {
            sch.awaitThreadResult(msg.id());
        } else {
            sch.getFunctionResult(msg, 10000);
        }
    }
}

TEST_CASE_METHOD(SlowExecutorTestFixture,
                 "Test unregistering host",
                 "[scheduler]")
{
    faabric::util::setMockMode(true);

    std::string thisHost = faabric::util::getSystemConfig().endpointHost;
    std::string otherHost = "foobar";
    sch.addHostToGlobalSet(otherHost);

    int nCores = 5;
    faabric::HostResources res;
    res.set_slots(nCores);
    sch.setThisHostResources(res);

    // Set up capacity for other host
    faabric::scheduler::queueResourceResponse(otherHost, res);

    std::shared_ptr<faabric::BatchExecuteRequest> req =
      faabric::util::batchExecFactory("foo", "bar", nCores + 1);
    sch.callFunctions(req);
    faabric::Message msg = req->messages().at(0);

    // Check other host is added
    const std::set<std::string>& expectedHosts = { otherHost };
    REQUIRE(sch.getFunctionRegisteredHosts(msg.user(), msg.function()) ==
            expectedHosts);
    REQUIRE(sch.getFunctionRegisteredHostCount(msg) == 1);

    // Remove host for another function and check host isn't removed
    faabric::Message otherMsg = faabric::util::messageFactory("foo", "qux");
    sch.removeRegisteredHost(otherHost, otherMsg.user(), otherMsg.function());
    REQUIRE(sch.getFunctionRegisteredHosts(msg.user(), msg.function()) ==
            expectedHosts);
    REQUIRE(sch.getFunctionRegisteredHostCount(msg) == 1);

    // Remove host
    sch.removeRegisteredHost(otherHost, msg.user(), msg.function());
    REQUIRE(sch.getFunctionRegisteredHosts(msg.user(), msg.function()).empty());
    REQUIRE(sch.getFunctionRegisteredHostCount(msg) == 0);
}

TEST_CASE_METHOD(SlowExecutorTestFixture, "Check test mode", "[scheduler]")
{
    auto reqA = faabric::util::batchExecFactory("demo", "echo", 1);
    auto& msgA = *reqA->mutable_messages(0);
    auto reqB = faabric::util::batchExecFactory("demo", "echo", 1);
    auto& msgB = *reqB->mutable_messages(0);
    auto reqC = faabric::util::batchExecFactory("demo", "echo", 1);
    auto& msgC = *reqC->mutable_messages(0);

    SECTION("No test mode")
    {
        faabric::util::setTestMode(false);

        sch.callFunctions(reqA);
        REQUIRE(sch.getRecordedMessagesAll().empty());
    }

    SECTION("Test mode")
    {
        faabric::util::setTestMode(true);

        sch.callFunctions(reqA);
        sch.callFunctions(reqB);
        sch.callFunctions(reqC);

        std::vector<int> expectedIds = { msgA.id(), msgB.id(), msgC.id() };
        std::vector<faabric::Message> actual = sch.getRecordedMessagesAll();

        REQUIRE(actual.size() == expectedIds.size());
        for (int i = 0; i < expectedIds.size(); i++) {
            REQUIRE(expectedIds.at(i) == actual.at(i).id());
        }
    }
}

TEST_CASE_METHOD(SlowExecutorTestFixture,
                 "Test getting a function result",
                 "[scheduler]")
{
    // Request function
    std::string funcName = "my func";
    std::string userName = "some user";
    std::string inputData = "blahblah";
    faabric::Message call = faabric::util::messageFactory(userName, funcName);
    call.set_inputdata(inputData);

    sch.setFunctionResult(call);

    // Check retrieval method gets the same call out again
    faabric::Message actualCall2 = sch.getFunctionResult(call, 1);

    checkMessageEquality(call, actualCall2);
}

TEST_CASE_METHOD(SlowExecutorTestFixture,
                 "Check multithreaded function results",
                 "[scheduler]")
{
    int nWaiters = 10;
    int nWaiterMessages = 4;
    conf.overrideCpuCount = nWaiters * nWaiterMessages;

    std::vector<std::jthread> waiterThreads;

    // Create waiters that will submit messages and await their results
    for (int i = 0; i < nWaiters; i++) {
        waiterThreads.emplace_back([nWaiterMessages] {
            Scheduler& sch = scheduler::getScheduler();

            std::shared_ptr<faabric::BatchExecuteRequest> req =
              faabric::util::batchExecFactory("demo", "echo", nWaiterMessages);
            int appId = req->messages(0).appid();
            std::vector<int> msgIds;
            std::for_each(req->mutable_messages()->begin(),
                          req->mutable_messages()->end(),
                          [&msgIds](auto msg) { msgIds.push_back(msg.id()); });

            // Invoke and await
            sch.callFunctions(req);
            for (auto msgId : msgIds) {
                sch.getFunctionResult(appId, msgId, 5000);
            }
        });
    }

    // Wait for all the threads to finish
    for (auto& w : waiterThreads) {
        if (w.joinable()) {
            w.join();
        }
    }
}

TEST_CASE_METHOD(SlowExecutorTestFixture,
                 "Check getting function status",
                 "[scheduler]")
{
    std::string expectedOutput;
    int expectedReturnValue = 0;
    faabric::Message_MessageType expectedType;
    std::string expectedHost = faabric::util::getSystemConfig().endpointHost;

    faabric::Message msg;
    SECTION("Running")
    {
        msg = faabric::util::messageFactory("demo", "echo");
        expectedReturnValue = 0;
        expectedType = faabric::Message_MessageType_EMPTY;
        expectedHost = "";
    }

    SECTION("Failure")
    {
        msg = faabric::util::messageFactory("demo", "echo");

        expectedOutput = "I have failed";
        msg.set_outputdata(expectedOutput);
        msg.set_returnvalue(1);
        sch.setFunctionResult(msg);

        expectedReturnValue = 1;
        expectedType = faabric::Message_MessageType_CALL;
    }

    SECTION("Success")
    {
        msg = faabric::util::messageFactory("demo", "echo");

        expectedOutput = "I have succeeded";
        msg.set_outputdata(expectedOutput);
        msg.set_returnvalue(0);
        sch.setFunctionResult(msg);

        expectedReturnValue = 0;
        expectedType = faabric::Message_MessageType_CALL;
    }

    // Check status when nothing has been written
    const faabric::Message result = sch.getFunctionResult(msg, 0);

    REQUIRE(result.returnvalue() == expectedReturnValue);
    REQUIRE(result.type() == expectedType);
    REQUIRE(result.outputdata() == expectedOutput);
    REQUIRE(result.executedhost() == expectedHost);
}

TEST_CASE_METHOD(SlowExecutorTestFixture,
                 "Check logging chained functions",
                 "[scheduler]")
{
    auto ber = faabric::util::batchExecFactory("demo", "echo", 4);
    faabric::Message& msg = *ber->mutable_messages(0);
    faabric::Message& chainedMsgA = *ber->mutable_messages(1);
    faabric::Message& chainedMsgB = *ber->mutable_messages(2);
    faabric::Message& chainedMsgC = *ber->mutable_messages(3);

    // We need to set the function result in order to get the chained
    // functions. We can do so multiple times
    sch.setFunctionResult(msg);

    // Check empty initially
    REQUIRE(faabric::util::getChainedFunctions(msg).empty());

    // Log and check this shows up in the result
    faabric::util::logChainedFunction(msg, chainedMsgA);
    std::set<unsigned int> expected = { (unsigned int)chainedMsgA.id() };

    sch.setFunctionResult(msg);
    REQUIRE(faabric::util::getChainedFunctions(msg) == expected);

    // Log some more and check
    faabric::util::logChainedFunction(msg, chainedMsgA);
    faabric::util::logChainedFunction(msg, chainedMsgB);
    faabric::util::logChainedFunction(msg, chainedMsgC);
    expected = { (unsigned int)chainedMsgA.id(),
                 (unsigned int)chainedMsgB.id(),
                 (unsigned int)chainedMsgC.id() };

    sch.setFunctionResult(msg);
    REQUIRE(faabric::util::getChainedFunctions(msg) == expected);
}

TEST_CASE_METHOD(SlowExecutorTestFixture,
                 "Test non-master batch request returned to master",
                 "[scheduler]")
{
    faabric::util::setMockMode(true);

    std::string otherHost = "other";

    std::shared_ptr<faabric::BatchExecuteRequest> req =
      faabric::util::batchExecFactory("blah", "foo", 1);
    req->mutable_messages()->at(0).set_masterhost(otherHost);

    faabric::util::SchedulingDecision decision = sch.callFunctions(req);
    REQUIRE(decision.hosts.empty());
    REQUIRE(decision.returnHost == otherHost);

    // Check forwarded to master
    auto actualReqs = faabric::scheduler::getBatchRequests();
    REQUIRE(actualReqs.size() == 1);
    REQUIRE(actualReqs.at(0).first == otherHost);
    REQUIRE(actualReqs.at(0).second->id() == req->id());
}

TEST_CASE_METHOD(SlowExecutorTestFixture,
                 "Test broadcast snapshot deletion",
                 "[scheduler]")
{
    faabric::util::setMockMode(true);

    // Set up other hosts
    std::string otherHostA = "otherA";
    std::string otherHostB = "otherB";
    std::string otherHostC = "otherC";

    sch.addHostToGlobalSet(otherHostA);
    sch.addHostToGlobalSet(otherHostB);
    sch.addHostToGlobalSet(otherHostC);

    int nCores = 3;
    faabric::HostResources res;
    res.set_slots(nCores);
    sch.setThisHostResources(res);

    // Set up capacity for other hosts
    faabric::scheduler::queueResourceResponse(otherHostA, res);
    faabric::scheduler::queueResourceResponse(otherHostB, res);
    faabric::scheduler::queueResourceResponse(otherHostC, res);

    // Set up a number of requests that will use this host and two others, but
    // not the third
    faabric::Message msg = faabric::util::messageFactory("foo", "bar");
    int nRequests = 2 * nCores + 1;
    std::shared_ptr<faabric::BatchExecuteRequest> req =
      faabric::util::batchExecFactory("foo", "bar", nRequests);

    sch.callFunctions(req);

    // Check other hosts are added
    REQUIRE(sch.getFunctionRegisteredHostCount(msg) == 2);

    std::set<std::string> expectedHosts =
      sch.getFunctionRegisteredHosts(msg.user(), msg.function());

    std::string snapKey = "blahblah";

    // Broadcast deletion of some snapshot
    sch.broadcastSnapshotDelete(msg, snapKey);

    std::vector<std::pair<std::string, std::string>> expectedDeleteRequests;
    for (auto h : expectedHosts) {
        expectedDeleteRequests.push_back({ h, snapKey });
    };
    auto actualDeleteRequests = faabric::snapshot::getSnapshotDeletes();

    REQUIRE(actualDeleteRequests == expectedDeleteRequests);
}

TEST_CASE_METHOD(SlowExecutorTestFixture,
                 "Test set thread results on remote host",
                 "[scheduler]")
{
    faabric::util::setMockMode(true);

    faabric::Message msg = faabric::util::messageFactory("foo", "bar");
    msg.set_masterhost("otherHost");

    // Set the thread result
    int returnValue = 123;
    std::string snapKey;
    std::vector<faabric::util::SnapshotDiff> diffs;

    SECTION("Without diffs") {}

    SECTION("With diffs")
    {
        snapKey = "foobar123";
        std::vector<uint8_t> snapData = { 1, 2, 3 };
        // Push initial update
        diffs = std::vector<faabric::util::SnapshotDiff>(1);
        diffs.emplace_back(faabric::util::SnapshotDiff(
          faabric::util::SnapshotDataType::Raw,
          faabric::util::SnapshotMergeOperation::Bytewise,
          123,
          snapData));
    }

    sch.setThreadResult(msg, returnValue, snapKey, diffs);
    auto actualResults = faabric::snapshot::getThreadResults();

    REQUIRE(actualResults.size() == 1);
    REQUIRE(actualResults.at(0).first == "otherHost");

    auto actualRes = actualResults.at(0).second;
    REQUIRE(actualRes.msgId == msg.id());
    REQUIRE(actualRes.res == returnValue);
    REQUIRE(actualRes.key == snapKey);
    REQUIRE(actualRes.diffs.size() == diffs.size());
}

TEST_CASE_METHOD(DummyExecutorTestFixture, "Test executor reuse", "[scheduler]")
{
    std::shared_ptr<faabric::BatchExecuteRequest> reqA =
      faabric::util::batchExecFactory("foo", "bar", 2);
    auto reqAMsgIds = { reqA->messages(0).id(), reqA->messages(1).id() };
    std::shared_ptr<faabric::BatchExecuteRequest> reqB =
      faabric::util::batchExecFactory("foo", "bar", 2);
    auto reqBMsgIds = { reqB->messages(0).id(), reqB->messages(1).id() };

    faabric::Message msgA = reqA->mutable_messages()->at(0);
    faabric::Message msgB = reqB->mutable_messages()->at(0);

    // Execute a couple of functions
    sch.callFunctions(reqA);
    for (auto msgId : reqAMsgIds) {
        faabric::Message res =
          sch.getFunctionResult(msgA.appid(), msgId, SHORT_TEST_TIMEOUT_MS);
        REQUIRE(res.returnvalue() == 0);
    }

    // Check executor count
    REQUIRE(sch.getFunctionExecutorCount(msgA) == 2);

    // Execute a couple more functions
    sch.callFunctions(reqB);
    for (auto msgId : reqBMsgIds) {
        faabric::Message res =
          sch.getFunctionResult(msgB.appid(), msgId, SHORT_TEST_TIMEOUT_MS);
        REQUIRE(res.returnvalue() == 0);
    }

    // Check executor count is still the same
    REQUIRE(sch.getFunctionExecutorCount(msgA) == 2);
    REQUIRE(sch.getFunctionExecutorCount(msgB) == 2);
}

TEST_CASE_METHOD(DummyExecutorTestFixture,
                 "Test point-to-point mappings sent from scheduler",
                 "[scheduler]")
{
    faabric::util::setMockMode(true);

    std::string thisHost = conf.endpointHost;
    std::string otherHost = "foobar";

    faabric::transport::PointToPointBroker& broker =
      faabric::transport::getPointToPointBroker();

    sch.addHostToGlobalSet(otherHost);

    // Set resources for this host
    int nSlotsThisHost = 2;
    faabric::HostResources resourcesThisHost;
    resourcesThisHost.set_slots(nSlotsThisHost);
    sch.setThisHostResources(resourcesThisHost);

    // Set resources for other host
    int nSlotsOtherHost = 5;
    faabric::HostResources resourcesOtherHost;
    resourcesOtherHost.set_slots(nSlotsOtherHost);
    faabric::scheduler::queueResourceResponse(otherHost, resourcesOtherHost);

    // Set up request
    auto req = faabric::util::batchExecFactory("foo", "bar", 4);
    faabric::Message& firstMsg = req->mutable_messages()->at(0);

    int appId = firstMsg.appid();
    int groupId = 0;
    int groupSize = 10;
    bool forceLocal = false;
    bool expectMappingsSent = false;

    SECTION("No group ID")
    {
        groupId = 0;

        SECTION("Force local")
        {
            forceLocal = true;
            expectMappingsSent = false;
        }

        SECTION("No force local")
        {
            forceLocal = false;
            expectMappingsSent = false;
        }
    }

    SECTION("With group ID")
    {
        groupId = 123;

        SECTION("Force local")
        {
            forceLocal = true;
            expectMappingsSent = false;
        }

        SECTION("No force local")
        {
            forceLocal = false;
            expectMappingsSent = true;
        }
    }

    // Set up the group
    if (groupId > 0) {
        faabric::transport::PointToPointGroup::addGroup(
          appId, groupId, groupSize);
    }

    // Build expectation
    std::vector<std::string> expectedHosts = {
        thisHost, thisHost, otherHost, otherHost
    };
    if (forceLocal) {
        expectedHosts = { thisHost, thisHost, thisHost, thisHost };
    }

    faabric::util::SchedulingDecision expectedDecision(appId, groupId);

    std::vector<int> msgIds;
    for (int i = 0; i < req->messages().size(); i++) {
        faabric::Message& m = req->mutable_messages()->at(i);
        m.set_groupid(groupId);
        m.set_groupidx(i);

        expectedDecision.addMessage(expectedHosts.at(i), req->messages().at(i));

        msgIds.push_back(m.id());
    }

    if (forceLocal) {
        req->mutable_messages()->at(0).set_topologyhint("FORCE_LOCAL");
    }

    // Schedule and check decision
    faabric::util::SchedulingDecision actualDecision = sch.callFunctions(req);
    checkSchedulingDecisionEquality(expectedDecision, actualDecision);

    // Check mappings set up locally or not
    std::set<int> registeredIdxs = broker.getIdxsRegisteredForGroup(groupId);
    if (expectMappingsSent) {
        REQUIRE(registeredIdxs.size() == 4);
    } else {
        REQUIRE(registeredIdxs.empty());
    }

    // Check mappings sent or not
    std::vector<std::pair<std::string, faabric::PointToPointMappings>>
      sentMappings = faabric::transport::getSentMappings();

    if (expectMappingsSent) {
        REQUIRE(sentMappings.size() == 1);
        REQUIRE(sentMappings.at(0).first == otherHost);
    } else {
        REQUIRE(sentMappings.empty());
    }

    // Wait for the functions on this host to complete
    for (int i = 0; i < expectedHosts.size(); i++) {
        if (expectedHosts.at(i) != thisHost) {
            continue;
        }

        sch.getFunctionResult(appId, msgIds.at(i), 10000);
    }
}

TEST_CASE_METHOD(DummyExecutorTestFixture,
                 "Test scheduler register and deregister threads",
                 "[scheduler]")
{
    uint32_t msgIdA = 123;
    uint32_t msgIdB = 124;

    // Check empty initially
    REQUIRE(sch.getRegisteredThreads().empty());

    // Register a couple and check they're listed
    sch.registerThread(msgIdA);
    sch.registerThread(msgIdB);

    std::vector<uint32_t> expected = { msgIdA, msgIdB };
    REQUIRE(sch.getRegisteredThreads() == expected);

    // Deregister and check
    sch.deregisterThread(msgIdB);
    expected = { msgIdA };
    REQUIRE(sch.getRegisteredThreads() == expected);
}

TEST_CASE_METHOD(DummyExecutorTestFixture,
                 "Test caching message data when setting thread result",
                 "[scheduler]")
{
    // In here we want to check that data cached in the scheduler from a message
    // will survive the original message going out of scope
    uint8_t* msgData = nullptr;
    int bufferSize = 100;

    REQUIRE(sch.getCachedMessageCount() == 0);

    // Do everything in a nested scope
    {
        // Create a message
        faabric::transport::Message msg(bufferSize);

        // Get a pointer to the message data
        msgData = msg.udata().data();

        // Write something
        msgData[0] = 1;
        msgData[1] = 2;
        msgData[2] = 3;

        // Register a thread
        uint32_t msgId = 123;
        sch.registerThread(msgId);

        // Set result along with the message to cache
        sch.setThreadResultLocally(msgId, 0, msg);
    }

    // Now check that it's cached
    REQUIRE(sch.getCachedMessageCount() == 1);

    // Check we can still read from the message
    REQUIRE(msgData[0] == 1);
    REQUIRE(msgData[1] == 2);
    REQUIRE(msgData[2] == 3);
}
}
