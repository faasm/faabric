#include <catch2/catch.hpp>

#include "DummyExecutorFactory.h"
#include "faabric_utils.h"
#include "fixtures.h"

#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/executor/ExecutorFactory.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/redis/Redis.h>
#include <faabric/scheduler/FunctionCallClient.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/snapshot/SnapshotClient.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/transport/PointToPointBroker.h>
#include <faabric/transport/PointToPointClient.h>
#include <faabric/util/ExecGraph.h>
#include <faabric/util/environment.h>
#include <faabric/util/func.h>
#include <faabric/util/gids.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>
#include <faabric/util/snapshot.h>
#include <faabric/util/testing.h>

using namespace faabric::scheduler;

namespace tests {

class SlowExecutor final : public faabric::executor::Executor
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

    void setMemorySize(size_t newSize) override
    {
        SPDLOG_DEBUG("Setting dummy memory size");
    }
};

class SlowExecutorFactory : public faabric::executor::ExecutorFactory
{
  protected:
    std::shared_ptr<faabric::executor::Executor> createExecutor(
      faabric::Message& msg) override
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
        std::shared_ptr<faabric::executor::ExecutorFactory> fac =
          std::make_shared<SlowExecutorFactory>();
        setExecutorFactory(fac);
    };

    ~SlowExecutorTestFixture()
    {
        std::shared_ptr<faabric::executor::DummyExecutorFactory> fac =
          std::make_shared<faabric::executor::DummyExecutorFactory>();
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
        std::shared_ptr<faabric::executor::ExecutorFactory> fac =
          std::make_shared<faabric::executor::DummyExecutorFactory>();
        setExecutorFactory(fac);
    };

    ~DummyExecutorTestFixture()
    {
        std::shared_ptr<faabric::executor::DummyExecutorFactory> fac =
          std::make_shared<faabric::executor::DummyExecutorFactory>();
        setExecutorFactory(fac);
    };
};

TEST_CASE_METHOD(SlowExecutorTestFixture,
                 "Test scheduler clear-up",
                 "[scheduler]")
{
    std::string thisHost = conf.endpointHost;

    // Set resources
    int nCores = 5;
    faabric::HostResources res;
    res.set_slots(nCores);
    sch.setThisHostResources(res);

    // Set request
    int nCalls = nCores;
    auto req = faabric::util::batchExecFactory("blah", "foo", nCalls);
    auto msg = req->messages(0);

    // Initial checks
    REQUIRE(sch.getFunctionExecutorCount(msg) == 0);

    // Make calls with one extra that should be sent to the other host
    plannerCli.callFunctions(req);
    plannerCli.getMessageResult(msg, 500);

    REQUIRE(sch.getFunctionExecutorCount(msg) == nCores);

    sch.reset();

    // Check scheduler has been cleared
    REQUIRE(sch.getFunctionExecutorCount(msg) == 0);
}

TEST_CASE_METHOD(SlowExecutorTestFixture,
                 "Test batch scheduling",
                 "[scheduler]")
{
    std::string expectedSnapshot;
    faabric::BatchExecuteRequest::BatchExecuteType execMode;
    int32_t expectedSubType;
    std::string expectedContextData;

    int thisCores = 20;
    faabric::HostResources thisResources;
    thisResources.set_slots(thisCores);
    sch.setThisHostResources(thisResources);

    int nCallsOne = 5;
    int nCallsTwo = 10;

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

    std::string thisHost = conf.endpointHost;

    // Set up the messages
    std::vector<int> reqOneMsgIds;
    faabric::batch_scheduler::SchedulingDecision expectedDecisionOne(
      firstMsg.appid(), firstMsg.groupid());
    for (int i = 0; i < nCallsOne; i++) {
        // Set snapshot key
        faabric::Message& msg = reqOne->mutable_messages()->at(i);

        if (!isThreads) {
            msg.set_snapshotkey(expectedSnapshot);
        }

        // Set app index
        msg.set_appidx(i);

        expectedDecisionOne.addMessage(thisHost, msg);

        reqOneMsgIds.push_back(msg.id());
    }

    // Schedule the functions
    reqOne->set_type(execMode);
    reqOne->set_subtype(expectedSubType);
    reqOne->set_contextdata(expectedContextData);

    // Set the singlehost flag to avoid sending snapshots to the planner
    reqOne->set_singlehosthint(true);

    auto actualDecisionOne = plannerCli.callFunctions(reqOne);

    // Check decision is as expected
    checkSchedulingDecisionEquality(actualDecisionOne, expectedDecisionOne);

    // Await the results
    if (isThreads) {
        sch.awaitThreadResults(reqOne);
    } else {
        for (int i = 0; i < nCallsOne; i++) {
            plannerCli.getMessageResult(appId, reqOneMsgIds.at(i), 500);
        }
    }

    // Check the executor counts on this host
    faabric::Message m = reqOne->messages().at(0);
    if (isThreads) {
        // For threads we expect only one executor
        REQUIRE(sch.getFunctionExecutorCount(m) == 1);
    } else {
        // For functions we expect one per core
        REQUIRE(sch.getFunctionExecutorCount(m) == nCallsOne);
    }

    // Check the number of messages executed locally and remotely
    REQUIRE(sch.getRecordedMessages().size() == nCallsOne);

    // Now schedule a second batch and check the decision
    std::shared_ptr<faabric::BatchExecuteRequest> reqTwo =
      faabric::util::batchExecFactory("foo", "bar", nCallsTwo);
    int appId2 = reqTwo->appid();

    std::vector<int> reqTwoMsgIds;
    const faabric::Message& firstMsg2 = reqTwo->messages().at(0);
    faabric::batch_scheduler::SchedulingDecision expectedDecisionTwo(
      appId2, firstMsg2.groupid());
    for (int i = 0; i < nCallsTwo; i++) {
        faabric::Message& msg = reqTwo->mutable_messages()->at(i);

        msg.set_appidx(i);

        if (!isThreads) {
            msg.set_snapshotkey(expectedSnapshot);
        }

        expectedDecisionTwo.addMessage(thisHost, msg);

        reqTwoMsgIds.push_back(msg.id());
    }

    // Create the batch request
    reqTwo->set_type(execMode);

    // Set the singlehost flag to avoid sending snapshots to the planner
    reqTwo->set_singlehosthint(true);

    // Schedule the functions
    auto actualDecisionTwo = plannerCli.callFunctions(reqTwo);

    // Check scheduling decision
    checkSchedulingDecisionEquality(actualDecisionTwo, expectedDecisionTwo);

    // Await the results
    if (isThreads) {
        sch.awaitThreadResults(reqTwo);
    } else {
        for (int i = 0; i < nCallsTwo; i++) {
            plannerCli.getMessageResult(appId2, reqTwoMsgIds.at(i), 10000);
        }
    }

    // Check no other functions have been scheduled on this host
    REQUIRE(sch.getRecordedMessages().size() == nCallsOne + nCallsTwo);

    if (isThreads) {
        REQUIRE(sch.getFunctionExecutorCount(m) == 1);
    } else {
        REQUIRE(sch.getFunctionExecutorCount(m) == nCallsTwo);
    }
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

        plannerCli.callFunctions(reqA);
        plannerCli.getMessageResult(msgA, 500);
        REQUIRE(sch.getRecordedMessages().empty());
    }

    SECTION("Test mode")
    {
        faabric::util::setTestMode(true);

        plannerCli.callFunctions(reqA);
        plannerCli.callFunctions(reqB);
        plannerCli.callFunctions(reqC);

        plannerCli.getMessageResult(msgA, 500);
        plannerCli.getMessageResult(msgB, 500);
        plannerCli.getMessageResult(msgC, 500);

        std::vector<int> expectedIds = { msgA.id(), msgB.id(), msgC.id() };
        std::vector<faabric::Message> actual = sch.getRecordedMessages();

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
    auto req = faabric::util::batchExecFactory(userName, funcName, 1);
    auto& firstMsg = *req->mutable_messages(0);
    firstMsg.set_inputdata(inputData);
    firstMsg.set_executedhost(faabric::util::getSystemConfig().endpointHost);

    // If we want to set a function result, the planner must see at least one
    // slot, and at least one used slot in this host
    faabric::HostResources res;
    res.set_slots(1);
    res.set_usedslots(1);
    sch.setThisHostResources(res);

    plannerCli.setMessageResult(std::make_shared<Message>(firstMsg));

    // Check retrieval method gets the same call out again
    faabric::Message resultMsg = plannerCli.getMessageResult(firstMsg, 1);

    // Manually set the timestamp for the sent message so that the check
    // matches (the timestamp is set in the planner client)
    firstMsg.set_finishtimestamp(resultMsg.finishtimestamp());

    checkMessageEquality(firstMsg, resultMsg);
}

TEST_CASE_METHOD(SlowExecutorTestFixture,
                 "Check multithreaded function results",
                 "[scheduler]")
{
    int nWaiters = 10;
    int nWaiterMessages = 4;
    int nCores = nWaiters * nWaiterMessages;
    faabric::HostResources res;
    res.set_slots(nCores);
    sch.setThisHostResources(res);

    std::vector<std::jthread> waiterThreads;

    // Create waiters that will submit messages and await their results
    for (int i = 0; i < nWaiters; i++) {
        waiterThreads.emplace_back([nWaiterMessages] {
            auto& plannerCli = faabric::planner::getPlannerClient();

            std::shared_ptr<faabric::BatchExecuteRequest> req =
              faabric::util::batchExecFactory("demo", "echo", nWaiterMessages);
            int appId = req->messages(0).appid();
            std::vector<int> msgIds;
            std::for_each(req->mutable_messages()->begin(),
                          req->mutable_messages()->end(),
                          [&msgIds](auto msg) { msgIds.push_back(msg.id()); });

            // Invoke and await
            plannerCli.callFunctions(req);
            for (auto msgId : msgIds) {
                plannerCli.getMessageResult(appId, msgId, 5000);
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

    // If we want to set a function result, the planner must see at least one
    // slot, and at least one used slot in this host
    faabric::HostResources res;
    res.set_slots(1);
    res.set_usedslots(1);
    sch.setThisHostResources(res);

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
        msg.set_executedhost(expectedHost);
        plannerCli.setMessageResult(std::make_shared<Message>(msg));

        expectedReturnValue = 1;
        expectedType = faabric::Message_MessageType_CALL;
    }

    SECTION("Success")
    {
        msg = faabric::util::messageFactory("demo", "echo");

        expectedOutput = "I have succeeded";
        msg.set_outputdata(expectedOutput);
        msg.set_returnvalue(0);
        msg.set_executedhost(expectedHost);
        plannerCli.setMessageResult(std::make_shared<Message>(msg));

        expectedReturnValue = 0;
        expectedType = faabric::Message_MessageType_CALL;
    }

    // Check status when nothing has been written
    const faabric::Message result = plannerCli.getMessageResult(msg, 1000);

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

    // If we want to set a function result, the planner must see at least one
    // slot, and at least one used slot in this host
    faabric::HostResources res;
    res.set_slots(8);
    res.set_usedslots(4);
    sch.setThisHostResources(res);

    // We need to set the function result in order to get the chained
    // functions. We can do so multiple times
    msg.set_executedhost(faabric::util::getSystemConfig().endpointHost);
    plannerCli.setMessageResult(std::make_shared<Message>(msg));

    // Check empty initially
    REQUIRE(faabric::util::getChainedFunctions(msg).empty());

    // Log and check this shows up in the result (change the message id as,
    // technically, messages should be unique, so setting the result a second
    // time for the same message is undefined behaviour)
    msg.set_id(faabric::util::generateGid());
    faabric::util::logChainedFunction(msg, chainedMsgA);
    std::set<unsigned int> expected = { (unsigned int)chainedMsgA.id() };

    plannerCli.setMessageResult(std::make_shared<Message>(msg));
    REQUIRE(faabric::util::getChainedFunctions(msg) == expected);

    // Log some more and check (update the message id again)
    msg.set_id(faabric::util::generateGid());
    faabric::util::logChainedFunction(msg, chainedMsgA);
    faabric::util::logChainedFunction(msg, chainedMsgB);
    faabric::util::logChainedFunction(msg, chainedMsgC);
    expected = { (unsigned int)chainedMsgA.id(),
                 (unsigned int)chainedMsgB.id(),
                 (unsigned int)chainedMsgC.id() };

    plannerCli.setMessageResult(std::make_shared<Message>(msg));
    REQUIRE(faabric::util::getChainedFunctions(msg) == expected);
}

/* TODO(thread-opt): we don't delete snapshots yet
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

    plannerCli.callFunctions(req);

    // Check other hosts are added
    // REQUIRE(sch.getFunctionRegisteredHostCount(msg) == 2);

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
*/

TEST_CASE_METHOD(SlowExecutorTestFixture,
                 "Test set thread results on remote host",
                 "[scheduler]")
{
    faabric::util::setMockMode(true);

    const std::string otherHost = "otherHost";
    faabric::Message msg = faabric::util::messageFactory("foo", "bar");
    msg.set_mainhost(otherHost);
    msg.set_executedhost(faabric::util::getSystemConfig().endpointHost);

    auto fac = faabric::executor::getExecutorFactory();
    auto exec = fac->createExecutor(msg);

    // If we want to set a function result, the planner must see at least one
    // slot, and at least one used slot in this host. Both for the task
    // executed in "otherHost" (executed as part of createExecutor) as well
    // as the one we are setting the result for
    faabric::HostResources res;
    res.set_slots(2);
    res.set_usedslots(2);
    sch.setThisHostResources(res);
    // Resources for the background task
    sch.addHostToGlobalSet(otherHost, std::make_shared<HostResources>(res));

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

    exec->setThreadResult(msg, returnValue, snapKey, diffs);
    auto actualResults = faabric::snapshot::getThreadResults();

    REQUIRE(actualResults.size() == 1);
    REQUIRE(actualResults.at(0).first == "otherHost");

    auto actualRes = actualResults.at(0).second;
    REQUIRE(actualRes.msgId == msg.id());
    REQUIRE(actualRes.res == returnValue);
    REQUIRE(actualRes.key == snapKey);
    REQUIRE(actualRes.diffs.size() == diffs.size());

    exec->shutdown();
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
    plannerCli.callFunctions(reqA);
    for (auto msgId : reqAMsgIds) {
        faabric::Message res = plannerCli.getMessageResult(
          msgA.appid(), msgId, SHORT_TEST_TIMEOUT_MS);
        REQUIRE(res.returnvalue() == 0);
    }

    // Check executor count
    REQUIRE(sch.getFunctionExecutorCount(msgA) == 2);

    // Execute a couple more functions
    plannerCli.callFunctions(reqB);
    for (auto msgId : reqBMsgIds) {
        faabric::Message res = plannerCli.getMessageResult(
          msgB.appid(), msgId, SHORT_TEST_TIMEOUT_MS);
        REQUIRE(res.returnvalue() == 0);
    }

    // Check executor count is still the same
    REQUIRE(sch.getFunctionExecutorCount(msgA) == 2);
    REQUIRE(sch.getFunctionExecutorCount(msgB) == 2);
}

TEST_CASE_METHOD(DummyExecutorTestFixture,
                 "Test point-to-point mappings are sent",
                 "[scheduler]")
{
    faabric::util::setMockMode(true);

    std::string thisHost = conf.endpointHost;
    std::string otherHost = LOCALHOST;

    faabric::transport::PointToPointBroker& broker =
      faabric::transport::getPointToPointBroker();

    // Set resources for this host
    int nSlotsThisHost = 2;
    faabric::HostResources resourcesThisHost;
    resourcesThisHost.set_slots(nSlotsThisHost);
    sch.setThisHostResources(resourcesThisHost);

    // Set resources for other host
    int nSlotsOtherHost = 2;
    faabric::HostResources resourcesOtherHost;
    resourcesOtherHost.set_slots(nSlotsOtherHost);
    sch.addHostToGlobalSet(otherHost,
                           std::make_shared<HostResources>(resourcesOtherHost));

    // Set up request
    int numMessages = 4;
    auto req = faabric::util::batchExecFactory("foo", "bar", numMessages);
    for (int i = 0; i < numMessages; i++) {
        req->mutable_messages(i)->set_groupidx(i);
    }
    faabric::Message& firstMsg = req->mutable_messages()->at(0);
    int appId = firstMsg.appid();

    // Schedule and check decision
    auto actualDecision = plannerCli.callFunctions(req);
    int groupId = actualDecision.groupId;

    // Build expectation
    std::vector<std::string> expectedHosts = {
        thisHost, thisHost, otherHost, otherHost
    };

    faabric::batch_scheduler::SchedulingDecision expectedDecision(appId,
                                                                  groupId);

    std::vector<int> msgIds;
    for (int i = 0; i < req->messages().size(); i++) {
        faabric::Message& m = req->mutable_messages()->at(i);
        m.set_groupid(groupId);
        m.set_groupidx(i);

        expectedDecision.addMessage(expectedHosts.at(i), req->messages().at(i));

        msgIds.push_back(m.id());
    }

    checkSchedulingDecisionEquality(expectedDecision, actualDecision);

    // Check mappings set up locally or not
    std::set<int> registeredIdxs = broker.getIdxsRegisteredForGroup(groupId);
    REQUIRE(registeredIdxs.size() == 4);

    // Check mappings sent
    auto sentMappings = faabric::transport::getSentMappings();
    REQUIRE(sentMappings.size() == 1);
    REQUIRE(sentMappings.at(0).first == otherHost);
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
        int appId = 1;
        uint32_t msgId = 123;

        // Set result along with the message to cache
        sch.setThreadResultLocally(appId, msgId, 0, msg);
    }

    // Now check that it's cached
    REQUIRE(sch.getCachedMessageCount() == 1);

    // Check we can still read from the message
    REQUIRE(msgData[0] == 1);
    REQUIRE(msgData[1] == 2);
    REQUIRE(msgData[2] == 3);
}
}
