#include <catch2/catch.hpp>

#include "faabric_utils.h"

#include <sys/mman.h>

#include <faabric/proto/faabric.pb.h>
#include <faabric/redis/Redis.h>
#include <faabric/scheduler/ExecutorFactory.h>
#include <faabric/scheduler/FunctionCallClient.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/snapshot/SnapshotClient.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/util/config.h>
#include <faabric/util/dirty.h>
#include <faabric/util/func.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>
#include <faabric/util/memory.h>
#include <faabric/util/testing.h>

using namespace faabric::scheduler;
using namespace faabric::util;

namespace tests {

// Some tests in here run for longer
#define LONG_TEST_TIMEOUT_MS 10000

std::atomic<int> restoreCount = 0;
std::atomic<int> resetCount = 0;

TestExecutor::TestExecutor(faabric::Message& msg)
  : Executor(msg)
{
    setUpDummyMemory(dummyMemorySize);
}

void TestExecutor::reset(faabric::Message& msg)
{
    SPDLOG_DEBUG("Resetting TestExecutor");
    resetCount += 1;
}

void TestExecutor::setUpDummyMemory(size_t memSize)
{
    SPDLOG_DEBUG("TestExecutor initialising memory size {}", memSize);
    dummyMemory = faabric::util::allocatePrivateMemory(memSize);
    dummyMemorySize = memSize;
}

void TestExecutor::restore(const std::string& snapshotKey)
{
    restoreCount += 1;

    auto snap = reg.getSnapshot(snapshotKey);
    if (dummyMemory == nullptr) {
        throw std::runtime_error(
          "Attempting to restore test executor with no memory set up");
    }

    snap->mapToMemory({ dummyMemory.get(), dummyMemorySize });
}

std::span<uint8_t> TestExecutor::getMemoryView()
{
    return { dummyMemory.get(), dummyMemorySize };
}

int32_t TestExecutor::executeTask(
  int threadPoolIdx,
  int msgIdx,
  std::shared_ptr<faabric::BatchExecuteRequest> reqOrig)
{
    faabric::Message& msg = reqOrig->mutable_messages()->at(msgIdx);
    std::string funcStr = faabric::util::funcToString(msg, true);

    bool isThread = reqOrig->type() == faabric::BatchExecuteRequest::THREADS;

    // Check we're being asked to execute the function we've bound to
    assert(msg.user() == boundMessage.user());
    assert(msg.function() == boundMessage.function());

    // Custom thread-check function
    if (msg.function() == "thread-check" && !isThread) {
        msg.set_outputdata(
          fmt::format("Threaded function {} executed successfully", msg.id()));

        // Set up the request
        int nThreads = 5;
        if (!msg.inputdata().empty()) {
            nThreads = std::stoi(msg.inputdata());
        }

        SPDLOG_DEBUG("TestExecutor spawning {} threads", nThreads);

        std::shared_ptr<faabric::BatchExecuteRequest> chainedReq =
          faabric::util::batchExecFactory("dummy", "thread-check", nThreads);
        chainedReq->set_type(faabric::BatchExecuteRequest::THREADS);

        for (int i = 0; i < chainedReq->messages_size(); i++) {
            faabric::Message& m = chainedReq->mutable_messages()->at(i);
            m.set_appid(msg.appid());
            m.set_appidx(i + 1);
        }

        // Call the threads
        std::vector<std::pair<uint32_t, int32_t>> results =
          executeThreads(chainedReq, {});

        // Await the results
        for (auto [mid, result] : results) {
            if (result != mid / 100) {
                SPDLOG_ERROR("TestExecutor got invalid thread result, {} != {}",
                             result,
                             mid / 100);
                return 1;
            }
        }

        SPDLOG_TRACE("TestExecutor got {} thread results",
                     chainedReq->messages_size());
        return 0;
    }

    if (msg.function() == "ret-one") {
        return 1;
    }

    if (msg.function() == "chain-check-a") {
        if (msg.inputdata() == "chained") {
            // Set up output data for the chained call
            msg.set_outputdata("chain-check-a successful");
        } else {
            // Chain this function and another
            std::shared_ptr<faabric::BatchExecuteRequest> reqThis =
              faabric::util::batchExecFactory("dummy", "chain-check-a", 1);
            reqThis->mutable_messages()->at(0).set_inputdata("chained");

            std::shared_ptr<faabric::BatchExecuteRequest> reqOther =
              faabric::util::batchExecFactory("dummy", "chain-check-b", 1);

            Scheduler& sch = getScheduler();
            sch.callFunctions(reqThis);
            sch.callFunctions(reqOther);

            for (const auto& m : reqThis->messages()) {
                faabric::Message res =
                  sch.getFunctionResult(m.id(), SHORT_TEST_TIMEOUT_MS);
                assert(res.outputdata() == "chain-check-a successful");
            }

            for (const auto& m : reqOther->messages()) {
                faabric::Message res =
                  sch.getFunctionResult(m.id(), SHORT_TEST_TIMEOUT_MS);
                assert(res.outputdata() == "chain-check-b successful");
            }

            msg.set_outputdata("All chain checks successful");
        }
        return 0;
    }

    if (msg.function() == "chain-check-b") {
        msg.set_outputdata("chain-check-b successful");
        return 0;
    }

    if (msg.function() == "snap-check") {
        // Modify a page of the dummy memory
        uint8_t pageIdx = threadPoolIdx;

        // Set up the data.
        // Note, avoid writing a zero here as the memory is already zeroed hence
        // it's not a change
        std::vector<uint8_t> data = { (uint8_t)(pageIdx + 1),
                                      (uint8_t)(pageIdx + 2),
                                      (uint8_t)(pageIdx + 3) };

        // Copy in the data
        size_t offset = (pageIdx * faabric::util::HOST_PAGE_SIZE);
        SPDLOG_DEBUG("TestExecutor modifying page {} of memory ({}-{})",
                     pageIdx,
                     offset,
                     offset + data.size());

        if (dummyMemorySize < offset + data.size()) {
            throw std::runtime_error(
              "TestExecutor memory not large enough for test");
        }

        if (dummyMemory == nullptr) {
            throw std::runtime_error("Trying to set null TestExecutor memory");
        }

        ::memcpy(dummyMemory.get() + offset, data.data(), data.size());
    }

    if (msg.function() == "echo") {
        msg.set_outputdata(msg.inputdata());
        return 0;
    }

    if (msg.function() == "error") {
        throw std::runtime_error("This is a test error");
    }

    if (reqOrig->type() == faabric::BatchExecuteRequest::THREADS) {
        SPDLOG_DEBUG("TestExecutor executing simple thread {}", msg.id());
        return msg.id() / 100;
    }

    // Default
    msg.set_outputdata(fmt::format("Simple function {} executed", msg.id()));

    return 0;
}

std::shared_ptr<Executor> TestExecutorFactory::createExecutor(
  faabric::Message& msg)
{
    return std::make_shared<TestExecutor>(msg);
}

class TestExecutorFixture
  : public SchedulerTestFixture
  , public RedisTestFixture
  , public ConfTestFixture
  , public SnapshotTestFixture
{
  public:
    TestExecutorFixture()
    {
        std::shared_ptr<TestExecutorFactory> fac =
          std::make_shared<TestExecutorFactory>();
        setExecutorFactory(fac);

        restoreCount = 0;
        resetCount = 0;
    }

    ~TestExecutorFixture() = default;

  protected:
    int snapshotNPages = 10;
    size_t snapshotSize = snapshotNPages * faabric::util::HOST_PAGE_SIZE;

    MemoryRegion dummyMemory;

    std::vector<std::string> executeWithTestExecutor(
      std::shared_ptr<faabric::BatchExecuteRequest> req,
      bool forceLocal)
    {
        // Create the main thread snapshot if we're executing threads directly
        if (req->type() == faabric::BatchExecuteRequest::THREADS) {
            faabric::Message& msg = req->mutable_messages()->at(0);
            std::string snapKey = faabric::util::getMainThreadSnapshotKey(msg);
            auto snap =
              std::make_shared<faabric::util::SnapshotData>(snapshotSize);
            reg.registerSnapshot(snapKey, snap);
        }

        conf.overrideCpuCount = 10;
        conf.boundTimeout = SHORT_TEST_TIMEOUT_MS;
        faabric::util::SchedulingTopologyHint topologyHint =
          faabric::util::SchedulingTopologyHint::NORMAL;

        if (forceLocal) {
            topologyHint = faabric::util::SchedulingTopologyHint::FORCE_LOCAL;
        }

        return sch.callFunctions(req, topologyHint).hosts;
    }
};

TEST_CASE_METHOD(TestExecutorFixture,
                 "Test executing simple function",
                 "[executor]")
{
    std::shared_ptr<BatchExecuteRequest> req =
      faabric::util::batchExecFactory("dummy", "simple", 1);
    uint32_t msgId = req->messages().at(0).id();

    REQUIRE(req->messages_size() == 1);

    std::vector<std::string> actualHosts = executeWithTestExecutor(req, false);
    std::vector<std::string> expectedHosts = { conf.endpointHost };
    REQUIRE(actualHosts == expectedHosts);

    faabric::Message result =
      sch.getFunctionResult(msgId, SHORT_TEST_TIMEOUT_MS);
    std::string expected = fmt::format("Simple function {} executed", msgId);
    REQUIRE(result.outputdata() == expected);

    // Check that restore has not been called
    REQUIRE(restoreCount == 0);
}

TEST_CASE_METHOD(TestExecutorFixture,
                 "Test executing function repeatedly and flushing",
                 "[executor]")
{
    // Set the bound timeout to something short so the test runs fast
    conf.boundTimeout = 100;

    int numRepeats = 10;
    for (int i = 0; i < numRepeats; i++) {
        std::shared_ptr<BatchExecuteRequest> req =
          faabric::util::batchExecFactory("dummy", "simple", 1);
        uint32_t msgId = req->messages().at(0).id();

        executeWithTestExecutor(req, false);
        faabric::Message result =
          sch.getFunctionResult(msgId, SHORT_TEST_TIMEOUT_MS);
        std::string expected =
          fmt::format("Simple function {} executed", msgId);
        REQUIRE(result.outputdata() == expected);

        // We sleep for the same timeout threads have, to force a race condition
        // between the scheduler's flush and the thread's own cleanup timeout
        SLEEP_MS(conf.boundTimeout);

        // Flush
        sch.flushLocally();
    }
}

TEST_CASE_METHOD(TestExecutorFixture,
                 "Test executing chained functions",
                 "[executor]")
{
    std::shared_ptr<BatchExecuteRequest> req =
      faabric::util::batchExecFactory("dummy", "chain-check-a", 1);
    uint32_t msgId = req->messages().at(0).id();

    std::vector<std::string> actualHosts = executeWithTestExecutor(req, false);
    std::vector<std::string> expectedHosts = { conf.endpointHost };
    REQUIRE(actualHosts == expectedHosts);

    faabric::Message result =
      sch.getFunctionResult(msgId, SHORT_TEST_TIMEOUT_MS);
    REQUIRE(result.outputdata() == "All chain checks successful");

    // Check that restore has not been called
    REQUIRE(restoreCount == 0);
}

TEST_CASE_METHOD(TestExecutorFixture,
                 "Test executing threads directly",
                 "[executor]")
{
    int nThreads = 0;
    bool singleHost = false;
    int expectedRestoreCount = 0;

    SECTION("Single host")
    {
        singleHost = true;
        expectedRestoreCount = 0;

        SECTION("Underloaded") { nThreads = 10; }

        SECTION("Overloaded") { nThreads = 200; }
    }

    SECTION("Non-single host")
    {
        singleHost = false;
        expectedRestoreCount = 1;

        SECTION("Underloaded") { nThreads = 10; }

        SECTION("Overloaded") { nThreads = 200; }
    }

    std::shared_ptr<BatchExecuteRequest> req =
      faabric::util::batchExecFactory("dummy", "blah", nThreads);
    req->set_type(faabric::BatchExecuteRequest::THREADS);
    req->set_singlehost(singleHost);

    std::vector<uint32_t> messageIds;
    for (int i = 0; i < nThreads; i++) {
        faabric::Message& msg = req->mutable_messages()->at(i);
        msg.set_appidx(i);

        messageIds.emplace_back(req->messages().at(i).id());
    }

    std::vector<std::string> actualHosts = executeWithTestExecutor(req, false);
    std::vector<std::string> expectedHosts(nThreads, conf.endpointHost);
    REQUIRE(actualHosts == expectedHosts);

    for (int i = 0; i < nThreads; i++) {
        uint32_t msgId = messageIds.at(i);
        int32_t result = sch.awaitThreadResult(msgId);
        REQUIRE(result == msgId / 100);
    }

    // Check that restore has been called
    REQUIRE(restoreCount == expectedRestoreCount);
}

TEST_CASE_METHOD(TestExecutorFixture,
                 "Test executing chained threads",
                 "[executor]")
{
    int nThreads = 0;

    SECTION("Underloaded") { nThreads = 8; }

    SECTION("Overloaded") { nThreads = 100; }

    std::shared_ptr<BatchExecuteRequest> req =
      faabric::util::batchExecFactory("dummy", "thread-check", 1);
    faabric::Message& msg = req->mutable_messages()->at(0);
    msg.set_inputdata(std::to_string(nThreads));

    std::vector<std::string> actualHosts = executeWithTestExecutor(req, false);
    std::vector<std::string> expectedHosts = { conf.endpointHost };
    REQUIRE(actualHosts == expectedHosts);

    auto& sch = faabric::scheduler::getScheduler();
    faabric::Message res = sch.getFunctionResult(msg.id(), 5000);
    REQUIRE(res.returnvalue() == 0);

    // Check that restore hasn't been called, as all threads should be executed
    // on the same host
    REQUIRE(restoreCount == 0);
}

TEST_CASE_METHOD(TestExecutorFixture,
                 "Test repeatedly executing chained threads",
                 "[executor]")
{
    // We really want to stress things here, but it's quite quick to run, so
    // don't be afraid to bump up the number of threads
    int nRepeats = 10;
    int nThreads = 1000;

    std::shared_ptr<TestExecutorFactory> fac =
      std::make_shared<TestExecutorFactory>();
    setExecutorFactory(fac);

    conf.overrideCpuCount = 10;
    conf.boundTimeout = LONG_TEST_TIMEOUT_MS;

    for (int i = 0; i < nRepeats; i++) {
        std::shared_ptr<BatchExecuteRequest> req =
          faabric::util::batchExecFactory("dummy", "thread-check", 1);
        faabric::Message& msg = req->mutable_messages()->at(0);
        msg.set_inputdata(std::to_string(nThreads));

        std::vector<std::string> actualHosts =
          executeWithTestExecutor(req, false);
        std::vector<std::string> expectedHosts = { conf.endpointHost };
        REQUIRE(actualHosts == expectedHosts);

        faabric::Message res =
          sch.getFunctionResult(msg.id(), LONG_TEST_TIMEOUT_MS);
        REQUIRE(res.returnvalue() == 0);

        sch.reset();
    }
}

TEST_CASE_METHOD(TestExecutorFixture,
                 "Test thread results returned on non-master",
                 "[executor]")
{
    faabric::util::setMockMode(true);
    std::string otherHost = "other";

    int nThreads = 5;
    std::shared_ptr<BatchExecuteRequest> req =
      faabric::util::batchExecFactory("dummy", "blah", nThreads);
    req->set_type(faabric::BatchExecuteRequest::THREADS);

    std::vector<uint32_t> messageIds;
    for (int i = 0; i < nThreads; i++) {
        faabric::Message& msg = req->mutable_messages()->at(i);
        msg.set_masterhost(otherHost);

        messageIds.emplace_back(msg.id());
    }

    executeWithTestExecutor(req, true);

    // Note that because the results don't actually get logged on this host, we
    // can't wait on them as usual.
    auto actual = faabric::snapshot::getThreadResults();
    REQUIRE_RETRY(actual = faabric::snapshot::getThreadResults(),
                  actual.size() == nThreads);

    std::vector<uint32_t> actualMessageIds;
    for (auto& p : actual) {
        REQUIRE(p.first == otherHost);
        uint32_t messageId = p.second.first;
        int32_t returnValue = p.second.second;
        REQUIRE(returnValue == messageId / 100);

        actualMessageIds.push_back(messageId);
    }

    std::sort(actualMessageIds.begin(), actualMessageIds.end());
    std::sort(messageIds.begin(), messageIds.end());

    REQUIRE(actualMessageIds == messageIds);
}

TEST_CASE_METHOD(TestExecutorFixture, "Test non-zero return code", "[executor]")
{
    std::shared_ptr<BatchExecuteRequest> req =
      faabric::util::batchExecFactory("dummy", "ret-one", 1);
    faabric::Message& msg = req->mutable_messages()->at(0);

    executeWithTestExecutor(req, false);

    faabric::Message res = sch.getFunctionResult(msg.id(), 2000);
    REQUIRE(res.returnvalue() == 1);
}

TEST_CASE_METHOD(TestExecutorFixture, "Test erroring function", "[executor]")
{
    std::shared_ptr<BatchExecuteRequest> req =
      faabric::util::batchExecFactory("dummy", "error", 1);
    faabric::Message& msg = req->mutable_messages()->at(0);

    executeWithTestExecutor(req, false);

    faabric::Message res = sch.getFunctionResult(msg.id(), 2000);
    REQUIRE(res.returnvalue() == 1);

    std::string expectedErrorMsg = fmt::format(
      "Task {} threw exception. What: This is a test error", msg.id());
    REQUIRE(res.outputdata() == expectedErrorMsg);
}

TEST_CASE_METHOD(TestExecutorFixture, "Test erroring thread", "[executor]")
{
    std::shared_ptr<BatchExecuteRequest> req =
      faabric::util::batchExecFactory("dummy", "error", 1);
    faabric::Message& msg = req->mutable_messages()->at(0);
    req->set_type(faabric::BatchExecuteRequest::THREADS);

    executeWithTestExecutor(req, false);

    int32_t res = sch.awaitThreadResult(msg.id());
    REQUIRE(res == 1);
}

TEST_CASE_METHOD(TestExecutorFixture,
                 "Test executing different functions",
                 "[executor]")
{
    // Set up multiple requests
    std::shared_ptr<BatchExecuteRequest> reqA =
      faabric::util::batchExecFactory("dummy", "echo", 2);
    std::shared_ptr<BatchExecuteRequest> reqB =
      faabric::util::batchExecFactory("dummy", "ret-one", 1);
    std::shared_ptr<BatchExecuteRequest> reqC =
      faabric::util::batchExecFactory("dummy", "blah", 2);

    reqA->mutable_messages()->at(0).set_inputdata("Message A1");
    reqA->mutable_messages()->at(1).set_inputdata("Message A2");

    std::shared_ptr<TestExecutorFactory> fac =
      std::make_shared<TestExecutorFactory>();
    setExecutorFactory(fac);

    conf.overrideCpuCount = 10;
    conf.boundTimeout = SHORT_TEST_TIMEOUT_MS;

    // Execute all the functions
    sch.callFunctions(reqA);
    sch.callFunctions(reqB);
    sch.callFunctions(reqC);

    faabric::Message resA1 =
      sch.getFunctionResult(reqA->messages().at(0).id(), SHORT_TEST_TIMEOUT_MS);
    faabric::Message resA2 =
      sch.getFunctionResult(reqA->messages().at(1).id(), SHORT_TEST_TIMEOUT_MS);

    faabric::Message resB =
      sch.getFunctionResult(reqB->messages().at(0).id(), SHORT_TEST_TIMEOUT_MS);

    faabric::Message resC1 =
      sch.getFunctionResult(reqC->messages().at(0).id(), SHORT_TEST_TIMEOUT_MS);
    faabric::Message resC2 =
      sch.getFunctionResult(reqC->messages().at(1).id(), SHORT_TEST_TIMEOUT_MS);

    REQUIRE(resA1.outputdata() == "Message A1");
    REQUIRE(resA2.outputdata() == "Message A2");

    REQUIRE(resB.returnvalue() == 1);

    REQUIRE(resC1.outputdata() == fmt::format("Simple function {} executed",
                                              reqC->messages().at(0).id()));
    REQUIRE(resC2.outputdata() == fmt::format("Simple function {} executed",
                                              reqC->messages().at(1).id()));
}

TEST_CASE_METHOD(TestExecutorFixture,
                 "Test claiming and releasing executor",
                 "[executor]")
{
    faabric::Message msgA = faabric::util::messageFactory("foo", "bar");
    faabric::Message msgB = faabric::util::messageFactory("foo", "bar");

    std::shared_ptr<faabric::scheduler::ExecutorFactory> fac =
      faabric::scheduler::getExecutorFactory();
    std::shared_ptr<faabric::scheduler::Executor> execA =
      fac->createExecutor(msgA);
    std::shared_ptr<faabric::scheduler::Executor> execB =
      fac->createExecutor(msgB);

    // Claim one
    REQUIRE(execA->tryClaim());

    // Check can't claim again
    REQUIRE(!execA->tryClaim());

    // Same for the other
    REQUIRE(execB->tryClaim());
    REQUIRE(!execB->tryClaim());

    // Release one and check can claim again
    execA->releaseClaim();

    REQUIRE(execA->tryClaim());
    REQUIRE(!execB->tryClaim());
}

TEST_CASE_METHOD(TestExecutorFixture,
                 "Test executor timing out waiting",
                 "[executor]")
{
    std::shared_ptr<faabric::BatchExecuteRequest> req =
      faabric::util::batchExecFactory("foo", "bar", 1);
    faabric::Message& msg = req->mutable_messages()->at(0);

    // Set a very short bound timeout so we can check it works
    conf.boundTimeout = 300;

    auto& sch = faabric::scheduler::getScheduler();
    sch.callFunctions(req);

    REQUIRE(sch.getFunctionExecutorCount(msg) == 1);

    REQUIRE_RETRY({}, sch.getFunctionExecutorCount(msg) == 0);
}

TEST_CASE_METHOD(TestExecutorFixture,
                 "Test snapshot diffs returned to master",
                 "[executor]")
{
    int nThreads = 4;

    // Sanity check memory size
    REQUIRE(TEST_EXECUTOR_DEFAULT_MEMORY_SIZE > nThreads * HOST_PAGE_SIZE);

    std::shared_ptr<faabric::BatchExecuteRequest> req =
      faabric::util::batchExecFactory("dummy", "snap-check", nThreads);
    req->set_type(faabric::BatchExecuteRequest::THREADS);

    faabric::util::setMockMode(true);
    std::string otherHost = "other";

    std::string mainThreadSnapshotKey =
      faabric::util::getMainThreadSnapshotKey(req->mutable_messages()->at(0));

    // Set up some messages executing with a different master host
    std::vector<uint32_t> messageIds;
    for (int i = 0; i < nThreads; i++) {
        faabric::Message& msg = req->mutable_messages()->at(i);
        msg.set_masterhost(otherHost);
        msg.set_appidx(i);

        messageIds.emplace_back(msg.id());
    }

    executeWithTestExecutor(req, true);

    // Results aren't set on this host as it's not the master, so we have to
    // wait
    REQUIRE_RETRY({}, faabric::snapshot::getThreadResults().size() == nThreads);

    // Check results have been sent back to the master host
    auto actualResults = faabric::snapshot::getThreadResults();
    REQUIRE(actualResults.size() == nThreads);

    // Check diffs also sent
    auto diffs = faabric::snapshot::getSnapshotDiffPushes();
    REQUIRE(diffs.size() == 1);
    REQUIRE(diffs.at(0).first == otherHost);
    std::vector<faabric::util::SnapshotDiff> diffList = diffs.at(0).second;

    // Each thread should have edited one page, check diffs are correct
    REQUIRE(diffList.size() == nThreads);
    for (int i = 0; i < diffList.size(); i++) {
        // Check offset and data (according to logic defined in the dummy
        // executor)
        REQUIRE(diffList.at(i).getOffset() ==
                i * faabric::util::HOST_PAGE_SIZE);

        std::vector<uint8_t> expected = { (uint8_t)(i + 1),
                                          (uint8_t)(i + 2),
                                          (uint8_t)(i + 3) };

        std::vector<uint8_t> actual = diffList.at(i).getDataCopy();

        REQUIRE(actual == expected);
    }

    // Check no merge regions left on the snapshot
    REQUIRE(reg.getSnapshot(mainThreadSnapshotKey)->getMergeRegions().empty());
}

TEST_CASE_METHOD(TestExecutorFixture,
                 "Test snapshot diffs pushed to workers after initial snapshot",
                 "[executor]")
{
    faabric::util::setMockMode(true);

    std::string thisHost = conf.endpointHost;
    std::string otherHost = "other";
    sch.addHostToGlobalSet(otherHost);

    int nThreads = 5;

    // Execute some on this host, some on the others
    faabric::HostResources res;
    res.set_slots(2);
    sch.setThisHostResources(res);

    // Set up other host to have some resources
    faabric::HostResources resOther;
    resOther.set_slots(10);
    faabric::scheduler::queueResourceResponse(otherHost, resOther);

    // Set up message for a batch of threads
    std::shared_ptr<BatchExecuteRequest> req =
      faabric::util::batchExecFactory("dummy", "blah", nThreads);
    req->set_type(faabric::BatchExecuteRequest::THREADS);
    faabric::Message& msg = req->mutable_messages()->at(0);

    std::vector<uint32_t> messageIds;
    for (int i = 0; i < nThreads; i++) {
        faabric::Message& msg = req->mutable_messages()->at(i);
        messageIds.emplace_back(msg.id());
    }

    // Execute the functions
    std::vector<std::string> actualHosts = executeWithTestExecutor(req, false);

    // Check they're executed on the right hosts
    std::vector<std::string> expectedHosts = {
        thisHost, thisHost, otherHost, otherHost, otherHost
    };
    REQUIRE(actualHosts == expectedHosts);

    // Await local results
    for (int i = 0; i < actualHosts.size(); i++) {
        if (actualHosts.at(i) == thisHost) {
            sch.awaitThreadResult(messageIds.at(i));
        }
    }

    // Check the other host is registered
    std::set<std::string> expectedRegistered = { otherHost };
    REQUIRE(sch.getFunctionRegisteredHosts(msg) == expectedRegistered);

    // Check snapshot has been pushed
    auto pushes = faabric::snapshot::getSnapshotPushes();
    REQUIRE(pushes.size() == 1);
    REQUIRE(pushes.at(0).first == otherHost);
    REQUIRE(pushes.at(0).second->getSize() == snapshotSize);

    REQUIRE(faabric::snapshot::getSnapshotDiffPushes().empty());

    // Now reset snapshot pushes of all kinds
    faabric::snapshot::clearMockSnapshotRequests();

    // Update the snapshot and check we get expected diffs
    std::string mainThreadSnapshotKey =
      faabric::util::getMainThreadSnapshotKey(msg);
    auto snap = reg.getSnapshot(mainThreadSnapshotKey);
    int newValue = 8;
    snap->copyInData({ BYTES(&newValue), sizeof(int) });
    snap->copyInData({ BYTES(&newValue), sizeof(int) },
                     2 * faabric::util::HOST_PAGE_SIZE + 1);

    std::vector<faabric::util::SnapshotDiff> expectedDiffs =
      snap->getTrackedChanges();

    REQUIRE(expectedDiffs.size() == 2);

    // Set up another function, make sure they have the same app ID
    std::shared_ptr<BatchExecuteRequest> reqB =
      faabric::util::batchExecFactory("dummy", "blah", 1);
    reqB->set_type(faabric::BatchExecuteRequest::THREADS);

    faabric::Message& msgB = reqB->mutable_messages()->at(0);
    msgB.set_appid(msg.appid());

    REQUIRE(faabric::util::getMainThreadSnapshotKey(msgB) ==
            mainThreadSnapshotKey);

    // Invoke the function
    std::vector<std::string> expectedHostsB = { thisHost };
    std::vector<std::string> actualHostsB =
      executeWithTestExecutor(reqB, false);
    REQUIRE(actualHostsB == expectedHostsB);

    // Wait for it to finish locally
    sch.awaitThreadResult(msgB.id());

    // Check the full snapshot hasn't been pushed
    REQUIRE(faabric::snapshot::getSnapshotPushes().empty());

    // Check the diffs are pushed as expected
    auto diffPushes = faabric::snapshot::getSnapshotDiffPushes();
    REQUIRE(diffPushes.size() == 1);
    REQUIRE(diffPushes.at(0).first == otherHost);
    std::vector<faabric::util::SnapshotDiff> actualDiffs =
      diffPushes.at(0).second;

    for (int i = 0; i < actualDiffs.size(); i++) {
        REQUIRE(actualDiffs.at(i).getOffset() ==
                expectedDiffs.at(i).getOffset());
        REQUIRE(actualDiffs.at(i).getData().size() ==
                expectedDiffs.at(i).getData().size());
    }
}

TEST_CASE_METHOD(TestExecutorFixture,
                 "Test reset called for functions not threads",
                 "[executor]")
{
    faabric::util::setMockMode(true);

    conf.overrideCpuCount = 4;

    std::string hostOverride = conf.endpointHost;
    int nMessages = 1;
    faabric::BatchExecuteRequest::BatchExecuteType requestType =
      faabric::BatchExecuteRequest::FUNCTIONS;

    int expectedResets = 1;

    SECTION("Threads")
    {
        requestType = faabric::BatchExecuteRequest::THREADS;
        nMessages = 3;
        expectedResets = 0;
    }

    SECTION("Function") {}

    std::shared_ptr<BatchExecuteRequest> req =
      faabric::util::batchExecFactory("dummy", "blah", nMessages);
    req->set_type(requestType);

    faabric::Message& msg = req->mutable_messages()->at(0);

    if (requestType == faabric::BatchExecuteRequest::THREADS) {
        // Set up main thread snapshot
        size_t snapSize = TEST_EXECUTOR_DEFAULT_MEMORY_SIZE;
        auto snap = std::make_shared<SnapshotData>(snapSize);
        std::string snapKey = getMainThreadSnapshotKey(msg);

        reg.registerSnapshot(snapKey, snap);
    }

    for (auto& m : *req->mutable_messages()) {
        m.set_masterhost(hostOverride);
    }

    // Call functions and force to execute locally
    sch.callFunctions(req, faabric::util::SchedulingTopologyHint::FORCE_LOCAL);

    // Await execution
    for (auto& m : *req->mutable_messages()) {
        if (requestType == faabric::BatchExecuteRequest::THREADS) {
            sch.awaitThreadResult(m.id());
        } else {
            sch.getFunctionResult(m.id(), 2000);
        }
    }

    REQUIRE(resetCount == expectedResets);
}
}
