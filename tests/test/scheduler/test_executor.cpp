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

class TestExecutor final : public Executor
{
  public:
    TestExecutor(faabric::Message& msg)
      : Executor(msg)
    {}

    ~TestExecutor() {}

    uint8_t* dummyMemory = nullptr;
    size_t dummyMemorySize = 0;

    void postFinish() override
    {
        if (dummyMemory != nullptr) {
            munmap(dummyMemory, dummyMemorySize);
        }
    }

    void reset(faabric::Message& msg) override
    {
        SPDLOG_DEBUG("Resetting TestExecutor");
        resetCount += 1;
    }

    void restore(faabric::Message& msg) override
    {
        SPDLOG_DEBUG("Restoring TestExecutor");
        restoreCount += 1;

        // Initialise the dummy memory and map to snapshot
        faabric::snapshot::SnapshotRegistry& reg =
          faabric::snapshot::getSnapshotRegistry();
        faabric::util::SnapshotData& snap = reg.getSnapshot(msg.snapshotkey());

        // Note this has to be mmapped to be page-aligned
        dummyMemorySize = snap.size;
        dummyMemory = (uint8_t*)mmap(
          nullptr, snap.size, PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);

        reg.mapSnapshot(msg.snapshotkey(), dummyMemory);
    }

    faabric::util::SnapshotData snapshot() override
    {
        faabric::util::SnapshotData snap;
        snap.data = dummyMemory;
        snap.size = dummyMemorySize;
        return snap;
    }

    int32_t executeTask(
      int threadPoolIdx,
      int msgIdx,
      std::shared_ptr<faabric::BatchExecuteRequest> reqOrig) override
    {

        faabric::Message& msg = reqOrig->mutable_messages()->at(msgIdx);

        std::string funcStr = faabric::util::funcToString(msg, true);

        bool isThread =
          reqOrig->type() == faabric::BatchExecuteRequest::THREADS;

        // Check we're being asked to execute the function we've bound to
        assert(msg.user() == boundMessage.user());
        assert(msg.function() == boundMessage.function());

        // Custom thread-check function
        if (msg.function() == "thread-check" && !isThread) {
            msg.set_outputdata(fmt::format(
              "Threaded function {} executed successfully", msg.id()));

            // Set up the request
            int nThreads = 5;
            if (!msg.inputdata().empty()) {
                nThreads = std::stoi(msg.inputdata());
            }

            SPDLOG_DEBUG("TestExecutor spawning {} threads", nThreads);

            std::shared_ptr<faabric::BatchExecuteRequest> chainedReq =
              faabric::util::batchExecFactory(
                "dummy", "thread-check", nThreads);
            chainedReq->set_type(faabric::BatchExecuteRequest::THREADS);

            // Create a dummy snapshot
            std::string snapKey = funcStr + "-snap";
            faabric::snapshot::SnapshotRegistry& reg =
              faabric::snapshot::getSnapshotRegistry();
            faabric::util::SnapshotData snap;
            snap.data = new uint8_t[10];
            snap.size = 10;

            reg.takeSnapshot(snapKey, snap);

            for (int i = 0; i < chainedReq->messages_size(); i++) {
                faabric::Message& m = chainedReq->mutable_messages()->at(i);
                m.set_snapshotkey(snapKey);
                m.set_appidx(i + 1);
            }

            // Call the threads
            Scheduler& sch = getScheduler();
            sch.callFunctions(chainedReq);

            // Await the results
            for (const auto& msg : chainedReq->messages()) {
                uint32_t mid = msg.id();
                int threadRes = sch.awaitThreadResult(mid);

                if (threadRes != mid / 100) {
                    SPDLOG_ERROR(
                      "TestExecutor got invalid thread result, {} != {}",
                      threadRes,
                      mid / 100);
                    return 1;
                }
            }

            // Delete the snapshot
            delete[] snap.data;
            reg.deleteSnapshot(snapKey);

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

            faabric::util::SnapshotData& snapData =
              faabric::snapshot::getSnapshotRegistry().getSnapshot(
                msg.snapshotkey());

            // Avoid writing a zero here as the memory is already zeroed hence
            // it's not a change
            std::vector<uint8_t> data = { (uint8_t)(pageIdx + 1),
                                          (uint8_t)(pageIdx + 2),
                                          (uint8_t)(pageIdx + 3) };

            // Set up a merge region that should catch the diff
            size_t offset = (pageIdx * faabric::util::HOST_PAGE_SIZE);
            snapData.addMergeRegion(offset,
                                    data.size() + 10,
                                    SnapshotDataType::Raw,
                                    SnapshotMergeOperation::Overwrite);

            SPDLOG_DEBUG("TestExecutor modifying page {} of memory ({}-{})",
                         pageIdx,
                         offset,
                         offset + data.size());

            uint8_t* offsetPtr = dummyMemory + offset;
            std::memcpy(offsetPtr, data.data(), data.size());
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
        msg.set_outputdata(
          fmt::format("Simple function {} executed", msg.id()));

        return 0;
    }
};

class TestExecutorFactory : public ExecutorFactory
{
  protected:
    std::shared_ptr<Executor> createExecutor(faabric::Message& msg) override
    {
        return std::make_shared<TestExecutor>(msg);
    }
};

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

        setUpDummySnapshot();

        restoreCount = 0;
        resetCount = 0;
    }

    ~TestExecutorFixture() { munmap(snapshotData, snapshotSize); }

  protected:
    std::string snapshotKey = "foobar";
    uint8_t* snapshotData = nullptr;
    int snapshotNPages = 10;
    size_t snapshotSize = snapshotNPages * faabric::util::HOST_PAGE_SIZE;

    std::vector<std::string> executeWithTestExecutor(
      std::shared_ptr<faabric::BatchExecuteRequest> req,
      bool forceLocal)
    {
        conf.overrideCpuCount = 10;
        conf.boundTimeout = SHORT_TEST_TIMEOUT_MS;

        return sch.callFunctions(req, forceLocal).hosts;
    }

  private:
    void setUpDummySnapshot()
    {
        takeSnapshot(snapshotKey, snapshotNPages, true);
        faabric::util::resetDirtyTracking();
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

    SECTION("Underloaded") { nThreads = 10; }

    SECTION("Overloaded") { nThreads = 200; }

    std::shared_ptr<BatchExecuteRequest> req =
      faabric::util::batchExecFactory("dummy", "blah", nThreads);
    req->set_type(faabric::BatchExecuteRequest::THREADS);

    std::vector<uint32_t> messageIds;
    for (int i = 0; i < nThreads; i++) {
        faabric::Message& msg = req->mutable_messages()->at(i);
        msg.set_snapshotkey(snapshotKey);
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

    // Check that restore has not been called as we're on the master
    REQUIRE(restoreCount == 0);
}

TEST_CASE_METHOD(TestExecutorFixture,
                 "Test executing chained threads",
                 "[executor]")
{
    int nThreads;
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

    // Check that restore has not been called as we're on the master
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
                 "Test executing remote chained threads",
                 "[executor]")
{
    faabric::util::setMockMode(true);

    std::string thisHost = conf.endpointHost;

    // Add other host to available hosts
    std::string otherHost = "other";
    sch.addHostToGlobalSet(otherHost);

    // Make sure we have only enough resources to execute the initial function
    faabric::HostResources res;
    res.set_slots(1);
    sch.setThisHostResources(res);

    // Set up other host to have some resources
    faabric::HostResources resOther;
    resOther.set_slots(20);
    faabric::scheduler::queueResourceResponse(otherHost, resOther);

    // Background thread to execute main function and await results
    int nThreads = 8;
    auto latch = faabric::util::Latch::create(2);
    std::thread t([&latch, nThreads] {
        std::shared_ptr<BatchExecuteRequest> req =
          faabric::util::batchExecFactory("dummy", "thread-check", 1);
        faabric::Message& msg = req->mutable_messages()->at(0);
        msg.set_inputdata(std::to_string(nThreads));

        auto& sch = faabric::scheduler::getScheduler();
        sch.callFunctions(req, false);

        latch->wait();

        faabric::Message res = sch.getFunctionResult(msg.id(), 2000);
        assert(res.returnvalue() == 0);
    });

    // Wait for the remote thread request to have been submitted
    auto reqs = faabric::scheduler::getBatchRequests();
    REQUIRE_RETRY(reqs = faabric::scheduler::getBatchRequests(),
                  reqs.size() == 1);
    std::string actualHost = reqs.at(0).first;
    REQUIRE(actualHost == otherHost);

    std::shared_ptr<faabric::BatchExecuteRequest> distReq = reqs.at(0).second;
    REQUIRE(distReq->messages().size() == nThreads);
    faabric::Message firstMsg = distReq->messages().at(0);

    // Check restore hasn't been called (as we're the master)
    REQUIRE(restoreCount == 0);

    // Check the snapshot has been pushed to the other host
    auto snapPushes = faabric::snapshot::getSnapshotPushes();
    REQUIRE(snapPushes.size() == 1);
    REQUIRE(snapPushes.at(0).first == otherHost);

    // Now execute request on this host as if we were on the other host
    conf.endpointHost = otherHost;
    sch.callFunctions(distReq, true);

    // Check restore has been called as we're no longer master
    REQUIRE(restoreCount == 1);

    // Wait for the results
    auto results = faabric::snapshot::getThreadResults();
    REQUIRE_RETRY(results = faabric::snapshot::getThreadResults(),
                  results.size() == nThreads);

    // Reset the host config for this host
    conf.endpointHost = thisHost;

    // Process the thread results
    for (auto& r : results) {
        REQUIRE(r.first == thisHost);
        auto args = r.second;
        sch.setThreadResultLocally(args.first, args.second);
    }

    // Rejoin the background thread
    latch->wait();
    if (t.joinable()) {
        t.join();
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
        msg.set_snapshotkey(snapshotKey);
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

    msg.set_snapshotkey(snapshotKey);
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
    sch.callFunctions(reqA, false);
    sch.callFunctions(reqB, false);
    sch.callFunctions(reqC, false);

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
    std::shared_ptr<faabric::BatchExecuteRequest> req =
      faabric::util::batchExecFactory("dummy", "snap-check", nThreads);
    req->set_type(faabric::BatchExecuteRequest::THREADS);

    faabric::util::setMockMode(true);
    std::string otherHost = "other";

    // Set up some messages executing with a different master host
    std::vector<uint32_t> messageIds;
    for (int i = 0; i < nThreads; i++) {
        faabric::Message& msg = req->mutable_messages()->at(i);
        msg.set_snapshotkey(snapshotKey);
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
        REQUIRE(diffList.at(i).offset == i * faabric::util::HOST_PAGE_SIZE);

        std::vector<uint8_t> expected = { (uint8_t)(i + 1),
                                          (uint8_t)(i + 2),
                                          (uint8_t)(i + 3) };

        std::vector<uint8_t> actual(diffList.at(i).data,
                                    diffList.at(i).data + 3);

        REQUIRE(actual == expected);
    }
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

    // Execute a batch of threads
    std::shared_ptr<BatchExecuteRequest> req =
      faabric::util::batchExecFactory("dummy", "blah", nThreads);
    req->set_type(faabric::BatchExecuteRequest::THREADS);
    faabric::Message& msg = req->mutable_messages()->at(0);

    std::vector<uint32_t> messageIds;
    for (int i = 0; i < nThreads; i++) {
        faabric::Message& msg = req->mutable_messages()->at(i);
        msg.set_snapshotkey(snapshotKey);
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
    REQUIRE(pushes.at(0).first == otherHost);
    REQUIRE(pushes.at(0).second.size == snapshotSize);

    REQUIRE(faabric::snapshot::getSnapshotDiffPushes().empty());

    // Check that we're not registering any dirty pages on the snapshot
    faabric::util::SnapshotData& snap = reg.getSnapshot(snapshotKey);
    REQUIRE(snap.getDirtyPages().empty());

    // Now reset snapshot pushes of all kinds
    faabric::snapshot::clearMockSnapshotRequests();

    // Make an edit to the snapshot memory and get the expected diffs
    snap.data[0] = 9;
    snap.data[(2 * faabric::util::HOST_PAGE_SIZE) + 1] = 9;
    std::vector<faabric::util::SnapshotDiff> expectedDiffs =
      snap.getDirtyPages();
    REQUIRE(expectedDiffs.size() == 2);

    // Set up another function
    std::shared_ptr<BatchExecuteRequest> reqB =
      faabric::util::batchExecFactory("dummy", "blah", 1);
    reqB->set_type(faabric::BatchExecuteRequest::THREADS);
    reqB->mutable_messages()->at(0).set_snapshotkey(snapshotKey);

    // Invoke the function
    std::vector<std::string> expectedHostsB = { thisHost };
    std::vector<std::string> actualHostsB =
      executeWithTestExecutor(reqB, false);
    REQUIRE(actualHostsB == expectedHostsB);

    // Wait for it to finish locally
    sch.awaitThreadResult(reqB->mutable_messages()->at(0).id());

    // Check the full snapshot hasn't been pushed
    REQUIRE(faabric::snapshot::getSnapshotPushes().empty());

    // Check the diffs are pushed as expected
    auto diffPushes = faabric::snapshot::getSnapshotDiffPushes();
    REQUIRE(diffPushes.size() == 1);
    REQUIRE(diffPushes.at(0).first == otherHost);
    std::vector<faabric::util::SnapshotDiff> actualDiffs =
      diffPushes.at(0).second;

    for (int i = 0; i < actualDiffs.size(); i++) {
        REQUIRE(actualDiffs.at(i).offset == expectedDiffs.at(i).offset);
        REQUIRE(actualDiffs.at(i).size == expectedDiffs.at(i).size);
    }
}

TEST_CASE_METHOD(TestExecutorFixture,
                 "Test reset not called on master threads",
                 "[executor]")
{
    faabric::util::setMockMode(true);

    std::string hostOverride = conf.endpointHost;
    int nMessages = 1;
    faabric::BatchExecuteRequest::BatchExecuteType requestType =
      faabric::BatchExecuteRequest::FUNCTIONS;

    int expectedResets = 1;

    SECTION("Non-master threads")
    {
        hostOverride = "foobar";
        nMessages = 3;
        requestType = faabric::BatchExecuteRequest::THREADS;
    }

    SECTION("Master threads")
    {
        requestType = faabric::BatchExecuteRequest::THREADS;
        nMessages = 3;
        expectedResets = 0;
    }

    SECTION("Non-master function") { hostOverride = "foobar"; }

    SECTION("Master function") {}

    std::shared_ptr<BatchExecuteRequest> req =
      faabric::util::batchExecFactory("dummy", "blah", nMessages);
    req->set_type(requestType);

    std::vector<uint32_t> mids;
    for (auto& m : *req->mutable_messages()) {
        m.set_masterhost(hostOverride);
        mids.push_back(m.id());
    }

    // Call functions and force to execute locally
    sch.callFunctions(req, true);

    // As we're faking a non-master execution results will be sent back to
    // the fake master so we can't wait on them, thus have to sleep
    SLEEP_MS(1000);

    REQUIRE(resetCount == expectedResets);
}
}
