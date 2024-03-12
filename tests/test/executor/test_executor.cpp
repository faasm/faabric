#include <catch2/catch.hpp>

#include "fixtures.h"

#include <faabric/executor/ExecutorContext.h>
#include <faabric/executor/ExecutorFactory.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/redis/Redis.h>
#include <faabric/scheduler/FunctionCallClient.h>
#include <faabric/snapshot/SnapshotClient.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/util/config.h>
#include <faabric/util/dirty.h>
#include <faabric/util/func.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>
#include <faabric/util/memory.h>
#include <faabric/util/testing.h>

#include <sys/mman.h>

using namespace faabric::executor;
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
    Executor::reset(msg);

    SPDLOG_DEBUG("Resetting TestExecutor");
    resetCount += 1;
}

void TestExecutor::setUpDummyMemory(size_t memSize)
{
    SPDLOG_DEBUG("TestExecutor initialising memory size {}", memSize);
    dummyMemory = faabric::util::allocatePrivateMemory(memSize);
    dummyMemorySize = memSize;
}

size_t TestExecutor::getMaxMemorySize()
{
    return maxMemorySize;
}

void TestExecutor::restore(const std::string& snapshotKey)
{
    if (dummyMemory == nullptr) {
        throw std::runtime_error(
          "Attempting to restore test executor with no memory set up");
    }

    restoreCount += 1;
    Executor::restore(snapshotKey);
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
    auto& sch = faabric::scheduler::getScheduler();

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
        faabric::util::updateBatchExecAppId(chainedReq, reqOrig->appid());
        chainedReq->set_type(faabric::BatchExecuteRequest::THREADS);

        for (int i = 0; i < chainedReq->messages_size(); i++) {
            faabric::Message& m = chainedReq->mutable_messages()->at(i);
            m.set_appidx(i + 1);
        }

        // Call the threads
        if (reqOrig->singlehost()) {
            chainedReq->set_singlehost(true);
            for (int i = 0; i < chainedReq->messages_size(); i++) {
                chainedReq->mutable_messages(i)->set_executedhost(
                  faabric::util::getSystemConfig().endpointHost);
            }
            sch.executeBatch(chainedReq);
        } else {
            auto& plannerCli = faabric::planner::getPlannerClient();
            plannerCli.callFunctions(chainedReq);
        }

        // Await the results
        auto results = sch.awaitThreadResults(chainedReq);
        for (const auto& [mid, result] : results) {
            if (result != (mid / 100)) {
                SPDLOG_ERROR("TestExecutor got invalid thread result, {} != {}",
                             result,
                             msg.id() / 100);
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

            auto& plannerCli = faabric::planner::getPlannerClient();
            plannerCli.callFunctions(reqThis);
            plannerCli.callFunctions(reqOther);

            for (const auto& m : reqThis->messages()) {
                faabric::Message res =
                  plannerCli.getMessageResult(m, SHORT_TEST_TIMEOUT_MS);
                assert(res.outputdata() == "chain-check-a successful");
            }

            for (const auto& m : reqOther->messages()) {
                faabric::Message res =
                  plannerCli.getMessageResult(m, SHORT_TEST_TIMEOUT_MS);
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
        // Avoid writing a zero here as the memory is already zeroed hence it's
        // not a change
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

        ::memcpy(dummyMemory.get() + offset, data.data(), data.size());
        return 0;
    }

    if (msg.function() == "echo") {
        msg.set_outputdata(msg.inputdata());
        return 0;
    }

    if (msg.function() == "error") {
        throw std::runtime_error("This is a test error");
    }

    if (msg.function() == "sleep") {
        int timeToSleepMs = SHORT_TEST_TIMEOUT_MS;
        if (!msg.inputdata().empty()) {
            timeToSleepMs = std::stoi(msg.inputdata());
        }

        SPDLOG_DEBUG("Sleep test function going to sleep for {} ms",
                     timeToSleepMs);
        SLEEP_MS(timeToSleepMs);
        SPDLOG_DEBUG("Sleep test function waking up");

        msg.set_outputdata(
          fmt::format("Migration test function {} executed", msg.id()));

        return 0;
    }

    if (msg.function() == "single-host") {
        return 20;
    }

    if (msg.function() == "context-check") {
        std::shared_ptr<ExecutorContext> ctx = ExecutorContext::get();
        if (ctx == nullptr) {
            SPDLOG_ERROR("Executor context is null");
            return 999;
        }

        if (ctx->getExecutor() != this) {
            SPDLOG_ERROR("Executor not equal to this one");
            return 999;
        }

        if (ctx->getBatchRequest()->appid() != reqOrig->appid()) {
            SPDLOG_ERROR("Context request does not match ({} != {})",
                         ctx->getBatchRequest()->appid(),
                         reqOrig->appid());
            return 999;
        }

        if (ctx->getMsgIdx() != msgIdx) {
            SPDLOG_ERROR("Context message idx does not match ({} != {})",
                         ctx->getMsgIdx(),
                         msgIdx);
            return 999;
        }

        return 123;
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
  : public FunctionCallClientServerFixture
  , public SnapshotRegistryFixture
  , public ConfFixture
  , public SchedulerFixture
{
  public:
    TestExecutorFixture()
    {
        std::shared_ptr<TestExecutorFactory> fac =
          std::make_shared<TestExecutorFactory>();
        setExecutorFactory(fac);

        // Give enough resources for the tests
        faabric::HostResources thisResources;
        thisResources.set_slots(20);
        thisResources.set_usedslots(1);
        sch.setThisHostResources(thisResources);

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
        initThreadSnapshot(req);

        // If we are indicated to force local execution, we directly call the
        // scheduler method
        if (forceLocal) {
            // Set the executed host if executing locally (otherwise set in
            // the function call server)
            req->set_singlehost(true);
            for (int i = 0; i < req->messages_size(); i++) {
                req->mutable_messages(i)->set_executedhost(conf.endpointHost);
            }

            sch.executeBatch(req);
            return std::vector<std::string>(req->messages_size(),
                                            conf.endpointHost);
        }

        return plannerCli.callFunctions(req).hosts;
    }

    void initThreadSnapshot(std::shared_ptr<faabric::BatchExecuteRequest> req)
    {
        // Create the main thread snapshot if we're executing threads directly
        if (req->type() == faabric::BatchExecuteRequest::THREADS) {
            faabric::Message& msg = req->mutable_messages()->at(0);
            std::string snapKey = faabric::util::getMainThreadSnapshotKey(msg);
            auto snap =
              std::make_shared<faabric::util::SnapshotData>(snapshotSize);
            reg.registerSnapshot(snapKey, snap);
        }
    }
};

TEST_CASE_METHOD(TestExecutorFixture,
                 "Test executing simple function",
                 "[executor]")
{
    std::shared_ptr<BatchExecuteRequest> req =
      faabric::util::batchExecFactory("dummy", "simple", 1);
    const auto msg = req->messages().at(0);

    REQUIRE(req->messages_size() == 1);

    std::vector<std::string> actualHosts = executeWithTestExecutor(req, false);
    std::vector<std::string> expectedHosts = { conf.endpointHost };
    REQUIRE(actualHosts == expectedHosts);

    faabric::Message result =
      plannerCli.getMessageResult(msg, SHORT_TEST_TIMEOUT_MS);
    std::string expected = fmt::format("Simple function {} executed", msg.id());
    REQUIRE(result.outputdata() == expected);

    // Check that restore has not been called
    REQUIRE(restoreCount == 0);
}

TEST_CASE_METHOD(TestExecutorFixture,
                 "Test executing function repeatedly and flushing",
                 "[executor]")
{
    // Set the bound timeout to long enough that we don't end up flushing
    // between invocations
    conf.boundTimeout = 2000;

    int numRepeats = 10;
    for (int i = 0; i < numRepeats; i++) {
        std::shared_ptr<BatchExecuteRequest> req =
          faabric::util::batchExecFactory("dummy", "simple", 1);
        const auto msg = req->messages().at(0);

        executeWithTestExecutor(req, false);
        faabric::Message result =
          plannerCli.getMessageResult(msg, SHORT_TEST_TIMEOUT_MS);
        std::string expected =
          fmt::format("Simple function {} executed", msg.id());
        REQUIRE(result.outputdata() == expected);

        // Flush
        getExecutorFactory()->flushHost();
    }
}

TEST_CASE_METHOD(TestExecutorFixture,
                 "Test executing chained functions",
                 "[executor]")
{
    std::shared_ptr<BatchExecuteRequest> req =
      faabric::util::batchExecFactory("dummy", "chain-check-a", 1);
    const auto msg = req->messages().at(0);

    std::vector<std::string> actualHosts = executeWithTestExecutor(req, false);
    std::vector<std::string> expectedHosts = { conf.endpointHost };
    REQUIRE(actualHosts == expectedHosts);

    faabric::Message result =
      plannerCli.getMessageResult(msg, SHORT_TEST_TIMEOUT_MS);
    REQUIRE(result.outputdata() == "All chain checks successful");

    // Check that restore has not been called
    REQUIRE(restoreCount == 0);
}

TEST_CASE_METHOD(TestExecutorFixture,
                 "Test restore not called on single host",
                 "[executor]")
{
    int nThreads = 10;
    int expectedRestoreCount = 0;

    std::string thisHost = conf.endpointHost;
    std::string otherHost = "other";

    // Set resources
    HostResources localHost;
    localHost.set_slots(nThreads);
    localHost.set_usedslots(nThreads);
    sch.setThisHostResources(localHost);

    // Prepare request
    std::shared_ptr<BatchExecuteRequest> req =
      faabric::util::batchExecFactory("dummy", "blah", nThreads);
    req->set_type(faabric::BatchExecuteRequest::THREADS);

    SECTION("Single host")
    {
        expectedRestoreCount = 0;
        req->set_singlehost(true);
    }

    SECTION("Non-single host")
    {
        expectedRestoreCount = 1;
    }

    std::vector<std::string> expectedHosts;
    for (int i = 0; i < nThreads; i++) {
        expectedHosts.emplace_back(thisHost);
    }

    // Turn mock mode on to catch any cross-host messages
    setMockMode(true);

    // Execute the functions
    initThreadSnapshot(req);
    for (int i = 0; i < req->messages_size(); i++) {
        req->mutable_messages(i)->set_executedhost(conf.endpointHost);
    }
    sch.executeBatch(req);

    // Sleep to avoid not finding the messages when we query (otherwise, the
    // function call client/server will be mocked and we won't receive the
    // callback with the message result)
    SLEEP_MS(500);

    // Await the results on this host
    auto results = sch.awaitThreadResults(req);
    for (const auto& [mid, result] : results) {
        REQUIRE(result == (mid / 100));
    }

    // Check sent to other host if necessary
    auto batchRequests = faabric::scheduler::getBatchRequests();

    // Check the hosts match up
    REQUIRE(restoreCount == expectedRestoreCount);
}

TEST_CASE_METHOD(TestExecutorFixture,
                 "Test executing threads directly",
                 "[executor]")
{
    int nThreads = 0;

    SECTION("Underloaded")
    {
        nThreads = 10;
    }

    // Set resources
    HostResources localHost;
    localHost.set_slots(nThreads);
    localHost.set_usedslots(nThreads);
    sch.setThisHostResources(localHost);

    std::shared_ptr<BatchExecuteRequest> req =
      faabric::util::batchExecFactory("dummy", "blah", nThreads);
    req->set_type(faabric::BatchExecuteRequest::THREADS);

    std::vector<uint32_t> messageIds;
    for (int i = 0; i < nThreads; i++) {
        faabric::Message& msg = req->mutable_messages()->at(i);
        msg.set_appidx(i);
        msg.set_executedhost(faabric::util::getSystemConfig().endpointHost);

        messageIds.emplace_back(req->messages().at(i).id());
    }

    std::vector<std::string> actualHosts = executeWithTestExecutor(req, true);

    std::vector<std::string> expectedHosts(nThreads, conf.endpointHost);
    REQUIRE(actualHosts == expectedHosts);

    auto results = sch.awaitThreadResults(req);
    for (const auto& [mid, res] : results) {
        REQUIRE(res == mid / 100);
    }
}

TEST_CASE_METHOD(TestExecutorFixture,
                 "Test executing chained threads",
                 "[executor]")
{
    int nThreads = 8;

    // Set resources
    HostResources localHost;
    localHost.set_slots(nThreads + 1);
    localHost.set_usedslots(nThreads + 1);
    sch.setThisHostResources(localHost);

    auto req = faabric::util::batchExecFactory("dummy", "thread-check", 1);
    faabric::Message msg = req->messages(0);
    req->mutable_messages(0)->set_inputdata(std::to_string(nThreads));

    std::vector<std::string> actualHosts = executeWithTestExecutor(req, true);
    std::vector<std::string> expectedHosts = { conf.endpointHost };
    REQUIRE(actualHosts == expectedHosts);

    faabric::Message res = plannerCli.getMessageResult(msg, 5000);
    REQUIRE(res.returnvalue() == 0);
}

TEST_CASE_METHOD(TestExecutorFixture,
                 "Test repeatedly executing chained threads",
                 "[executor]")
{
    // We really want to stress things here, but it's quite quick to run, so
    // don't be afraid to bump up the number of threads
    int nRepeats = 10;
    int nThreads = 10;

    conf.overrideCpuCount = (nThreads + 1) * nRepeats;
    conf.boundTimeout = LONG_TEST_TIMEOUT_MS;
    faabric::HostResources hostResources;
    hostResources.set_slots(conf.overrideCpuCount);
    hostResources.set_usedslots(conf.overrideCpuCount);
    sch.setThisHostResources(hostResources);

    for (int i = 0; i < nRepeats; i++) {
        std::shared_ptr<BatchExecuteRequest> req =
          faabric::util::batchExecFactory("dummy", "thread-check", 1);
        faabric::Message msg = req->messages(0);
        req->mutable_messages(0)->set_inputdata(std::to_string(nThreads));

        auto actualHosts = executeWithTestExecutor(req, true);
        std::vector<std::string> expectedHosts = { conf.endpointHost };
        REQUIRE(actualHosts == expectedHosts);

        faabric::Message res =
          plannerCli.getMessageResult(msg, LONG_TEST_TIMEOUT_MS);
        REQUIRE(res.returnvalue() == 0);

        for (int mid : faabric::util::getChainedFunctions(msg)) {
            auto chainedRes = plannerCli.getMessageResult(
              msg.appid(), mid, LONG_TEST_TIMEOUT_MS);
            REQUIRE(chainedRes.returnvalue() == 0);
        }

        sch.reset();
    }
}

TEST_CASE_METHOD(TestExecutorFixture,
                 "Test thread results returned on non-main",
                 "[executor]")
{
    faabric::util::setMockMode(true);
    std::string otherHost = "other";

    int nThreads = 5;
    std::shared_ptr<BatchExecuteRequest> req =
      faabric::util::batchExecFactory("dummy", "blah", nThreads);
    req->set_type(faabric::BatchExecuteRequest::THREADS);

    // Set resources
    HostResources localHost;
    localHost.set_slots(nThreads);
    localHost.set_usedslots(nThreads);
    sch.setThisHostResources(localHost);

    std::vector<uint32_t> messageIds;
    for (int i = 0; i < nThreads; i++) {
        faabric::Message& msg = req->mutable_messages()->at(i);
        msg.set_mainhost(otherHost);

        messageIds.emplace_back(msg.id());
    }

    executeWithTestExecutor(req, true);

    // Note that because the results don't actually get logged on this host,
    // we can't wait on them as usual.
    auto actual = faabric::snapshot::getThreadResults();
    REQUIRE_RETRY(actual = faabric::snapshot::getThreadResults(),
                  actual.size() == nThreads);

    std::vector<uint32_t> actualMessageIds;
    for (auto& p : actual) {
        REQUIRE(p.first == otherHost);
        uint32_t messageId = p.second.msgId;
        int32_t returnValue = p.second.res;
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
    faabric::Message msg = req->messages(0);

    executeWithTestExecutor(req, false);

    faabric::Message res = plannerCli.getMessageResult(msg, 2000);
    REQUIRE(res.returnvalue() == 1);
}

TEST_CASE_METHOD(TestExecutorFixture, "Test erroring function", "[executor]")
{
    std::shared_ptr<BatchExecuteRequest> req =
      faabric::util::batchExecFactory("dummy", "error", 1);
    faabric::Message msg = req->messages(0);

    executeWithTestExecutor(req, false);

    faabric::Message res = plannerCli.getMessageResult(msg, 2000);
    REQUIRE(res.returnvalue() == 1);

    std::string expectedErrorMsg = fmt::format(
      "Task {} threw exception. What: This is a test error", msg.id());
    REQUIRE(res.outputdata() == expectedErrorMsg);
}

TEST_CASE_METHOD(TestExecutorFixture, "Test erroring thread", "[executor]")
{
    std::shared_ptr<BatchExecuteRequest> req =
      faabric::util::batchExecFactory("dummy", "error", 1);
    faabric::Message msg = req->messages(0);
    req->set_type(faabric::BatchExecuteRequest::THREADS);

    // Set resources
    HostResources localHost;
    localHost.set_slots(10);
    localHost.set_usedslots(10);
    sch.setThisHostResources(localHost);
    executeWithTestExecutor(req, true);

    auto results = sch.awaitThreadResults(req);
    for (const auto& [mid, res] : results) {
        REQUIRE(res == 1);
    }
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
    plannerCli.callFunctions(reqA);
    plannerCli.callFunctions(reqB);
    plannerCli.callFunctions(reqC);

    faabric::Message resA1 = plannerCli.getMessageResult(reqA->messages().at(0),
                                                         SHORT_TEST_TIMEOUT_MS);
    faabric::Message resA2 = plannerCli.getMessageResult(reqA->messages().at(1),
                                                         SHORT_TEST_TIMEOUT_MS);

    faabric::Message resB = plannerCli.getMessageResult(reqB->messages().at(0),
                                                        SHORT_TEST_TIMEOUT_MS);

    faabric::Message resC1 = plannerCli.getMessageResult(reqC->messages().at(0),
                                                         SHORT_TEST_TIMEOUT_MS);
    faabric::Message resC2 = plannerCli.getMessageResult(reqC->messages().at(1),
                                                         SHORT_TEST_TIMEOUT_MS);

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

    std::shared_ptr<ExecutorFactory> fac = getExecutorFactory();
    std::shared_ptr<Executor> execA = fac->createExecutor(msgA);
    std::shared_ptr<Executor> execB = fac->createExecutor(msgB);

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

    execA->shutdown();
    execB->shutdown();
}

TEST_CASE_METHOD(TestExecutorFixture,
                 "Check last executed time updated on each execution",
                 "[executor]")
{
    auto req = faabric::util::batchExecFactory("foo", "bar");
    faabric::Message msg = req->messages(0);
    req->mutable_messages(0)->set_executedhost(
      faabric::util::getSystemConfig().endpointHost);

    HostResources localHost;
    localHost.set_slots(5);
    localHost.set_usedslots(5);
    sch.setThisHostResources(localHost);

    std::shared_ptr<ExecutorFactory> fac = getExecutorFactory();
    std::shared_ptr<Executor> exec = fac->createExecutor(msg);

    long millisA = exec->getMillisSinceLastExec();

    long sleepMillis = 100;
    SLEEP_MS(sleepMillis);

    long millisB = exec->getMillisSinceLastExec();

    // Check delay is roughly correct. Need to allow for some margin for error
    // on such short timescales
    REQUIRE(millisB > millisA);
    REQUIRE((millisB - millisA) > (sleepMillis - 10));

    exec->executeTasks({ 0 }, req);

    long millisC = exec->getMillisSinceLastExec();
    REQUIRE(millisC < millisB);

    // Wait for execution to finish
    plannerCli.getMessageResult(msg, 2000);

    exec->shutdown();
}

TEST_CASE_METHOD(TestExecutorFixture,
                 "Test snapshot diffs returned to main",
                 "[executor]")
{
    int nThreads = 4;

    // Give enough resources for the tests
    faabric::HostResources thisResources;
    thisResources.set_slots(nThreads);
    thisResources.set_usedslots(nThreads);
    sch.setThisHostResources(thisResources);

    SECTION("XOR diffs")
    {
        conf.diffingMode = "xor";
    }

    SECTION("Bytewise diffs")
    {
        conf.diffingMode = "bytewise";
    }

    // Sanity check memory size
    REQUIRE(TEST_EXECUTOR_DEFAULT_MEMORY_SIZE > nThreads * HOST_PAGE_SIZE);

    std::shared_ptr<faabric::BatchExecuteRequest> req =
      faabric::util::batchExecFactory("dummy", "snap-check", nThreads);
    req->set_type(faabric::BatchExecuteRequest::THREADS);

    faabric::util::setMockMode(true);
    std::string otherHost = "other";

    std::string mainThreadSnapshotKey =
      faabric::util::getMainThreadSnapshotKey(req->mutable_messages()->at(0));

    // Set up some messages executing with a different main host
    std::vector<uint32_t> messageIds;
    for (int i = 0; i < nThreads; i++) {
        faabric::Message& msg = req->mutable_messages()->at(i);
        msg.set_mainhost(otherHost);
        msg.set_executedhost(faabric::util::getSystemConfig().endpointHost);
        msg.set_appidx(i);

        messageIds.emplace_back(msg.id());
    }

    // Execute directly calling the scheduler
    initThreadSnapshot(req);
    sch.executeBatch(req);

    // Results aren't set on this host as it's not the main, so we have to
    // wait
    REQUIRE_RETRY({}, faabric::snapshot::getThreadResults().size() == nThreads);

    // Check results have been sent back to the main host
    auto actualResults = faabric::snapshot::getThreadResults();
    REQUIRE(actualResults.size() == nThreads);

    // Check that only one result has been assigned diffs
    faabric::snapshot::MockThreadResult diffRes;
    bool diffsFound = false;
    for (int i = 0; i < nThreads; i++) {
        faabric::snapshot::MockThreadResult res = actualResults[i].second;
        if (!res.diffs.empty()) {
            if (diffsFound) {
                FAIL("More than one thread result has diffs");
            } else {
                diffRes = res;
                diffsFound = true;

                // Check it was sent to the right host
                REQUIRE(actualResults[i].first == otherHost);
            }
        }
    }

    // Check that diffs were found
    REQUIRE(diffsFound);
    std::vector<faabric::util::SnapshotDiff> diffList = diffRes.diffs;

    // Each thread should have edited one page, check diffs are correct
    REQUIRE(diffList.size() == nThreads);
    for (int i = 0; i < diffList.size(); i++) {
        // Check offset and data (according to logic defined in the dummy
        // executor)
        REQUIRE(diffList.at(i).getOffset() ==
                i * faabric::util::HOST_PAGE_SIZE);

        std::vector<uint8_t> expected;
        if (conf.diffingMode == "xor") {
            // In XOR mode we'll get the whole page back with a modification at
            // the start
            expected = std::vector<uint8_t>(HOST_PAGE_SIZE, 0);
            expected[0] = i + 1;
            expected[1] = i + 2;
            expected[2] = i + 3;
        } else {
            expected = { (uint8_t)(i + 1), (uint8_t)(i + 2), (uint8_t)(i + 3) };
        }

        std::vector<uint8_t> actual = diffList.at(i).getDataCopy();

        REQUIRE(actual == expected);
    }

    // Check no merge regions left on the snapshot
    REQUIRE(reg.getSnapshot(mainThreadSnapshotKey)->getMergeRegions().empty());
}

/* TODO(thread-opt): currently, we push the full snapshot every time from the
 * planner (and not diffs on subsequent executions)
TEST_CASE_METHOD(TestExecutorFixture,
                 "Test snapshot diffs pushed to workers after initial snapshot",
                 "[executor]")
{
    faabric::util::setMockMode(true);

    std::string thisHost = conf.endpointHost;
    std::string otherHost = "other";
    sch.addHostToGlobalSet(otherHost);

    int nThreads = 5;

    // Set host resources to force execution on other host
    setHostResources({ thisHost, otherHost }, { 2, 10 }, { 0, 0 });

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

    // Reset host resources
    setHostResources({ thisHost, otherHost }, { 2, 10 }, { 0, 0 });

    // Set up another function, make sure they have the same app ID
    std::shared_ptr<BatchExecuteRequest> reqB =
      faabric::util::batchExecFactory("dummy", "blah", nThreads);
    reqB->set_type(faabric::BatchExecuteRequest::THREADS);

    for (auto& m : *reqB->mutable_messages()) {
        m.set_appid(msg.appid());

        REQUIRE(faabric::util::getMainThreadSnapshotKey(m) ==
                mainThreadSnapshotKey);
    }

    // Invoke the function
    std::vector<std::string> actualHostsB =
      executeWithTestExecutor(reqB, false);
    REQUIRE(actualHostsB == expectedHosts);

    // Await local results
    for (int i = 0; i < actualHostsB.size(); i++) {
        if (actualHostsB.at(i) == thisHost) {
            sch.awaitThreadResult(reqB->messages().at(i).id());
        }
    }

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
*/

TEST_CASE_METHOD(TestExecutorFixture,
                 "Test reset called for functions not threads",
                 "[executor]")
{
    std::string hostOverride = conf.endpointHost;
    int nMessages = 1;
    faabric::BatchExecuteRequest::BatchExecuteType requestType =
      faabric::BatchExecuteRequest::FUNCTIONS;

    int expectedResets = 1;

    std::string user = "dummy";
    std::string function = "blah";

    SECTION("Threads")
    {
        requestType = faabric::BatchExecuteRequest::THREADS;
        nMessages = 3;
        expectedResets = 0;
    }

    SECTION("Simple function") {}

    SECTION("Function that spawns threads")
    {
        function = "thread-check";
    }

    std::shared_ptr<BatchExecuteRequest> req =
      faabric::util::batchExecFactory(user, function, nMessages);
    req->set_type(requestType);

    faabric::Message msg = req->messages().at(0);

    if (requestType == faabric::BatchExecuteRequest::THREADS) {
        // Set up main thread snapshot
        size_t snapSize = TEST_EXECUTOR_DEFAULT_MEMORY_SIZE;
        auto snap = std::make_shared<SnapshotData>(snapSize);
        std::string snapKey = getMainThreadSnapshotKey(msg);

        reg.registerSnapshot(snapKey, snap);
    }

    int appId = msg.appid();
    std::vector<int> msgIds;
    for (auto& m : *req->mutable_messages()) {
        m.set_mainhost(hostOverride);
        m.set_executedhost(hostOverride);
        msgIds.push_back(m.id());
    }

    // Set resources
    HostResources localHost;
    localHost.set_slots(20);
    localHost.set_usedslots(10);
    sch.setThisHostResources(localHost);

    // Call functions
    if (requestType == faabric::BatchExecuteRequest::THREADS) {
        // For threads, we call directly the scheduler method (instead of
        // routing through the planner) to avoid sending/receiving snapshots
        initThreadSnapshot(req);
        sch.executeBatch(req);
    } else {
        plannerCli.callFunctions(req);
    }

    // Await execution
    if (requestType == faabric::BatchExecuteRequest::THREADS) {
        sch.awaitThreadResults(req);
    } else {
        for (auto msgId : msgIds) {
            plannerCli.getMessageResult(appId, msgId, 2000);
        }
    }

    REQUIRE(resetCount == expectedResets);
}

TEST_CASE_METHOD(TestExecutorFixture,
                 "Test single host flag passed to executor",
                 "[executor]")
{
    std::string thisHost = conf.endpointHost;
    std::string otherHost = "other-host";

    std::vector<std::string> singleHosts = {
        thisHost, thisHost, thisHost, thisHost
    };

    int nMessages = singleHosts.size();
    std::shared_ptr<BatchExecuteRequest> req =
      faabric::util::batchExecFactory("dummy", "single-host", nMessages);

    int appId = req->messages(0).appid();
    std::vector<int> msgIds;
    for (int i = 0; i < nMessages; i++) {
        msgIds.push_back(req->messages(i).id());
    }

    // Give enough resources for the tests
    faabric::HostResources thisResources;
    thisResources.set_slots(nMessages + 1);
    thisResources.set_usedslots(nMessages + 1);
    sch.setThisHostResources(thisResources);

    int expectedResult = 20;

    executeWithTestExecutor(req, true);

    // Await results on this host
    for (int i = 0; i < nMessages; i++) {
        if (singleHosts[i] == thisHost) {
            faabric::Message res =
              plannerCli.getMessageResult(appId, msgIds.at(i), 2000);

            // Check result as expected
            REQUIRE(res.returnvalue() == expectedResult);
        }
    }
}

TEST_CASE_METHOD(TestExecutorFixture,
                 "Test executor sees context",
                 "[executor]")
{
    int nMessages = 5;
    std::shared_ptr<BatchExecuteRequest> req =
      faabric::util::batchExecFactory("dummy", "context-check", nMessages);
    int expectedResult = 123;

    plannerCli.callFunctions(req);

    for (int i = 0; i < nMessages; i++) {
        faabric::Message res =
          plannerCli.getMessageResult(req->messages().at(i), 2000);

        REQUIRE(res.returnvalue() == expectedResult);
    }
}

TEST_CASE_METHOD(TestExecutorFixture, "Test executor restore", "[executor]")
{
    // Create a message
    std::string user = "foo";
    std::string function = "bar";
    faabric::Message m = faabric::util::messageFactory(user, function);

    // Create a snapshot
    std::string snapKey = faabric::util::getMainThreadSnapshotKey(m);
    auto snap = std::make_shared<faabric::util::SnapshotData>(snapshotSize);
    reg.registerSnapshot(snapKey, snap);

    // Modify the snapshot to check changes are propagated
    std::vector<uint8_t> dataA = { 0, 1, 2, 3 };
    std::vector<uint8_t> dataB = { 4, 5, 6 };
    size_t offsetA = HOST_PAGE_SIZE;
    size_t offsetB = 3 * HOST_PAGE_SIZE;
    snap->copyInData(dataA, offsetA);
    snap->copyInData(dataB, offsetB);

    // Create an executor
    std::shared_ptr<ExecutorFactory> fac = getExecutorFactory();
    std::shared_ptr<Executor> exec = fac->createExecutor(m);

    // Restore from snapshot
    exec->restore(snapKey);

    // Check size of restored memory is as expected
    std::span<uint8_t> memViewAfter = exec->getMemoryView();
    REQUIRE(memViewAfter.size() == snapshotSize);

    // Check data found in restored memory
    std::vector<uint8_t> actualDataA(memViewAfter.data() + offsetA,
                                     memViewAfter.data() + offsetA +
                                       dataA.size());
    std::vector<uint8_t> actualDataB(memViewAfter.data() + offsetB,
                                     memViewAfter.data() + offsetB +
                                       dataB.size());

    REQUIRE(actualDataA == dataA);
    REQUIRE(actualDataB == dataB);

    exec->shutdown();
}

TEST_CASE_METHOD(TestExecutorFixture,
                 "Test get main thread snapshot",
                 "[executor]")
{
    std::string user = "foo";
    std::string function = "bar";
    faabric::Message m = faabric::util::messageFactory(user, function);

    // Get the snapshot key
    std::string snapKey = faabric::util::getMainThreadSnapshotKey(m);

    // Create an executor
    std::shared_ptr<ExecutorFactory> fac = getExecutorFactory();
    std::shared_ptr<Executor> exec = fac->createExecutor(m);

    // Get a pointer to the TestExecutor so we can override the max memory
    auto testExec = std::static_pointer_cast<TestExecutor>(exec);
    size_t memSize = testExec->dummyMemorySize;

    SECTION("Non-existent, don't create")
    {
        REQUIRE_THROWS(exec->getMainThreadSnapshot(m, false));
    }

    SECTION("Non-existent, create")
    {
        size_t expectedSize = memSize;
        size_t expectedMaxSize = memSize;

        SECTION("No max mem size")
        {
            testExec->maxMemorySize = 0;
        }

        SECTION("Max mem size")
        {
            testExec->maxMemorySize = 2 * memSize;
            expectedMaxSize = 2 * memSize;
        }

        std::shared_ptr<SnapshotData> snap =
          exec->getMainThreadSnapshot(m, true);
        REQUIRE(snap->getSize() == expectedSize);
        REQUIRE(snap->getMaxSize() == expectedMaxSize);
    }

    SECTION("Existing")
    {
        // Create the snapshot manually
        auto existingSnap =
          std::make_shared<faabric::util::SnapshotData>(memSize);
        reg.registerSnapshot(snapKey, existingSnap);

        bool requestCreate = false;
        SECTION("Request create if not exist")
        {
            requestCreate = true;
        }

        SECTION("No request create if not exist")
        {
            requestCreate = false;
        }

        std::shared_ptr<SnapshotData> actualSnap =
          exec->getMainThreadSnapshot(m, requestCreate);
        REQUIRE(actualSnap->getSize() == memSize);
        REQUIRE(actualSnap->getMaxSize() == memSize);

        // Check they are actually the same
        REQUIRE(actualSnap == existingSnap);
    }

    testExec->shutdown();
}

TEST_CASE_METHOD(TestExecutorFixture,
                 "Test executor keeps track of chained messages",
                 "[executor]")
{
    auto req = faabric::util::batchExecFactory("hello", "world", 1);
    auto& firstMsg = *req->mutable_messages(0);

    // Create an executor
    auto fac = getExecutorFactory();
    auto exec = fac->createExecutor(firstMsg);

    // At the begining there are no chained messages
    REQUIRE(exec->getChainedMessageIds().empty());

    // Add a chained call
    faabric::Message chainedMsg =
      faabric::util::messageFactory("hello", "chained");
    exec->addChainedMessage(chainedMsg);
    std::set<unsigned int> expectedChainedMessageIds = {
        (uint32_t)chainedMsg.id()
    };
    REQUIRE(exec->getChainedMessageIds() == expectedChainedMessageIds);
    auto actualChainedMessage = exec->getChainedMessage(chainedMsg.id());
    checkMessageEquality(actualChainedMessage, chainedMsg);

    // Resetting the executor removes the chained messages
    exec->reset(firstMsg);
    REQUIRE(exec->getChainedMessageIds().empty());
}

TEST_CASE_METHOD(TestExecutorFixture,
                 "Test executing threads manually",
                 "[executor]")
{
    int nThreads = 5;

    // Set resources
    HostResources localHost;
    localHost.set_slots(2 * nThreads);
    localHost.set_usedslots(nThreads);
    sch.setThisHostResources(localHost);

    // Prepare request
    std::shared_ptr<BatchExecuteRequest> req =
      faabric::util::batchExecFactory("dummy", "blah", nThreads);
    req->set_type(faabric::BatchExecuteRequest::THREADS);
    // Set single-host to avoid any snapshot sending
    req->set_singlehosthint(true);

    auto decision = faabric::planner::getPlannerClient().callFunctions(req);
    auto results = faabric::scheduler::getScheduler().awaitThreadResults(
      req, 10 * faabric::util::getSystemConfig().boundTimeout);

    // Check results
    REQUIRE(results.size() == req->messages_size());
    for (const auto& [mid, res] : results) {
        REQUIRE(res == (mid / 100));
    }
}
}
