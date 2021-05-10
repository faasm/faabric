#include "faabric_utils.h"
#include <catch.hpp>

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/ExecutorFactory.h>
#include <faabric/scheduler/FunctionCallClient.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/scheduler/SnapshotClient.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/util/config.h>
#include <faabric/util/func.h>
#include <faabric/util/testing.h>

using namespace faabric::scheduler;

namespace tests {

// Some tests in here run for longer
#define LONG_TEST_TIMEOUT_MS 10000

std::atomic<int> restoreCount = 0;

std::string setUpDummySnapshot()
{
    std::vector<uint8_t> snapData = { 0, 1, 2, 3, 4 };
    faabric::snapshot::SnapshotRegistry& reg =
      faabric::snapshot::getSnapshotRegistry();
    faabric::util::SnapshotData snap;
    std::string snapKey = "foobar";
    snap.data = snapData.data();
    snap.size = snapData.size();

    reg.takeSnapshot(snapKey, snap);

    return snapKey;
}

class TestExecutor final : public Executor
{
  public:
    TestExecutor(const faabric::Message& msg)
      : Executor(msg)
    {}

    ~TestExecutor() {}

    void restore(const faabric::Message& msg) { restoreCount += 1; }

    int32_t executeTask(int threadPoolIdx,
                        int msgIdx,
                        std::shared_ptr<faabric::BatchExecuteRequest> reqOrig)
    {
        auto logger = faabric::util::getLogger();
        faabric::Message& msg = reqOrig->mutable_messages()->at(msgIdx);
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

            std::shared_ptr<faabric::BatchExecuteRequest> chainedReq =
              faabric::util::batchExecFactory(
                "dummy", "thread-check", nThreads);
            chainedReq->set_type(faabric::BatchExecuteRequest::THREADS);

            std::string snapKey = setUpDummySnapshot();

            for (int i = 0; i < chainedReq->messages_size(); i++) {
                faabric::Message& m = chainedReq->mutable_messages()->at(i);
                m.set_snapshotkey(snapKey);
                m.set_appindex(i + 1);
            }

            // Call the threads
            Scheduler& sch = getScheduler();
            sch.callFunctions(chainedReq);

            for (int i = 0; i < chainedReq->messages_size(); i++) {
                uint32_t mid = chainedReq->messages().at(i).id();
                sch.awaitThreadResult(mid);
            }

            logger->trace("TestExecutor got {} thread results",
                          chainedReq->messages_size());
            return 0;
        } else if (msg.function() == "chain-check-a") {
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

                for (auto& m : reqThis->messages()) {
                    faabric::Message res =
                      sch.getFunctionResult(m.id(), SHORT_TEST_TIMEOUT_MS);
                    assert(res.outputdata() == "chain-check-a successful");
                }

                for (auto& m : reqOther->messages()) {
                    faabric::Message res =
                      sch.getFunctionResult(m.id(), SHORT_TEST_TIMEOUT_MS);
                    assert(res.outputdata() == "chain-check-b successful");
                }

                msg.set_outputdata("All chain checks successful");
            }
        } else if (msg.function() == "chain-check-b") {
            msg.set_outputdata("chain-check-b successful");
        } else if (msg.function() == "echo") {
            msg.set_outputdata(msg.inputdata());
            return 0;
        } else if (msg.function() == "ret-one") {
            return 1;
        } else if (msg.function() == "error") {
            throw std::runtime_error("This is a test error");
        } else if (reqOrig->type() == faabric::BatchExecuteRequest::THREADS) {
            logger->debug("TestExecutor executing thread {}", msg.id());

            return msg.id() / 100;
        } else {
            msg.set_outputdata(
              fmt::format("Simple function {} executed", msg.id()));
        }

        return 0;
    }
};

class TestExecutorFactory : public ExecutorFactory
{
  protected:
    std::shared_ptr<Executor> createExecutor(
      const faabric::Message& msg) override
    {
        return std::make_shared<TestExecutor>(msg);
    }
};

void executeWithTestExecutor(std::shared_ptr<faabric::BatchExecuteRequest> req,
                             bool forceLocal)
{
    std::shared_ptr<TestExecutorFactory> fac =
      std::make_shared<TestExecutorFactory>();
    setExecutorFactory(fac);

    auto& conf = faabric::util::getSystemConfig();
    int boundOriginal = conf.boundTimeout;
    int overrideCpuOriginal = conf.overrideCpuCount;

    conf.overrideCpuCount = 10;
    conf.boundTimeout = SHORT_TEST_TIMEOUT_MS;

    auto& sch = faabric::scheduler::getScheduler();
    sch.callFunctions(req, forceLocal);

    conf.boundTimeout = boundOriginal;
    conf.overrideCpuCount = overrideCpuOriginal;
}

TEST_CASE("Test executing simple function", "[executor]")
{
    cleanFaabric();
    restoreCount = 0;

    std::shared_ptr<BatchExecuteRequest> req =
      faabric::util::batchExecFactory("dummy", "simple", 1);
    uint32_t msgId = req->messages().at(0).id();

    REQUIRE(req->messages_size() == 1);

    executeWithTestExecutor(req, false);

    auto& sch = faabric::scheduler::getScheduler();
    faabric::Message result =
      sch.getFunctionResult(msgId, SHORT_TEST_TIMEOUT_MS);
    std::string expected = fmt::format("Simple function {} executed", msgId);
    REQUIRE(result.outputdata() == expected);

    // Check that restore has not been called
    REQUIRE(restoreCount == 0);

    sch.shutdown();
}

TEST_CASE("Test executing chained functions", "[executor]")
{
    cleanFaabric();
    restoreCount = 0;

    std::shared_ptr<BatchExecuteRequest> req =
      faabric::util::batchExecFactory("dummy", "chain-check-a", 1);
    uint32_t msgId = req->messages().at(0).id();

    executeWithTestExecutor(req, false);

    auto& sch = faabric::scheduler::getScheduler();
    faabric::Message result =
      sch.getFunctionResult(msgId, SHORT_TEST_TIMEOUT_MS);
    REQUIRE(result.outputdata() == "All chain checks successful");

    // Check that restore has not been called
    REQUIRE(restoreCount == 0);
    sch.shutdown();
}

TEST_CASE("Test executing threads directly", "[executor]")
{
    cleanFaabric();
    restoreCount = 0;
    int nThreads = 0;

    SECTION("Underloaded") { nThreads = 10; }

    SECTION("Overloaded") { nThreads = 200; }

    std::shared_ptr<BatchExecuteRequest> req =
      faabric::util::batchExecFactory("dummy", "blah", nThreads);
    req->set_type(faabric::BatchExecuteRequest::THREADS);

    std::vector<uint32_t> messageIds;
    std::string snapKey = setUpDummySnapshot();
    for (int i = 0; i < nThreads; i++) {
        faabric::Message& msg = req->mutable_messages()->at(i);
        msg.set_snapshotkey(snapKey);
        msg.set_appindex(i);

        messageIds.emplace_back(req->messages().at(i).id());
    }

    executeWithTestExecutor(req, false);

    auto& sch = faabric::scheduler::getScheduler();
    for (int i = 0; i < nThreads; i++) {
        uint32_t msgId = messageIds.at(i);
        int32_t result = sch.awaitThreadResult(msgId);
        REQUIRE(result == msgId / 100);
    }

    // Check that restore has not been called as we're on the master
    REQUIRE(restoreCount == 0);

    sch.shutdown();
}

TEST_CASE("Test executing threads indirectly", "[executor]")
{
    cleanFaabric();
    restoreCount = 0;

    int nThreads;
    SECTION("Underloaded") { nThreads = 8; }

    SECTION("Overloaded") { nThreads = 100; }

    std::shared_ptr<BatchExecuteRequest> req =
      faabric::util::batchExecFactory("dummy", "thread-check", 1);
    faabric::Message& msg = req->mutable_messages()->at(0);
    msg.set_inputdata(std::to_string(nThreads));

    executeWithTestExecutor(req, false);

    auto& sch = faabric::scheduler::getScheduler();
    faabric::Message res = sch.getFunctionResult(msg.id(), 5000);
    REQUIRE(res.returnvalue() == 0);

    // Check that restore has not been called as we're on the master
    REQUIRE(restoreCount == 0);

    sch.shutdown();
}

TEST_CASE("Test repeatedly executing threads indirectly", "[executor]")
{
    cleanFaabric();

    // We really want to stress things here, but it's quite quick to run, so
    // don't be afraid to bump up the number of threads
    int nRepeats = 10;
    int nThreads = 1000;

    auto& sch = faabric::scheduler::getScheduler();

    std::shared_ptr<TestExecutorFactory> fac =
      std::make_shared<TestExecutorFactory>();
    setExecutorFactory(fac);

    auto& conf = faabric::util::getSystemConfig();
    int boundOriginal = conf.boundTimeout;
    int overrideCpuOriginal = conf.overrideCpuCount;

    conf.overrideCpuCount = 10;
    conf.boundTimeout = LONG_TEST_TIMEOUT_MS;

    for (int i = 0; i < nRepeats; i++) {
        std::shared_ptr<BatchExecuteRequest> req =
          faabric::util::batchExecFactory("dummy", "thread-check", 1);
        faabric::Message& msg = req->mutable_messages()->at(0);
        msg.set_inputdata(std::to_string(nThreads));

        executeWithTestExecutor(req, false);

        faabric::Message res =
          sch.getFunctionResult(msg.id(), LONG_TEST_TIMEOUT_MS);
        REQUIRE(res.returnvalue() == 0);

        sch.reset();
    }

    conf.boundTimeout = boundOriginal;
    conf.overrideCpuCount = overrideCpuOriginal;
}

TEST_CASE("Test executing remote threads indirectly", "[executor]")
{
    cleanFaabric();
    restoreCount = 0;

    std::shared_ptr<TestExecutorFactory> fac =
      std::make_shared<TestExecutorFactory>();
    setExecutorFactory(fac);

    faabric::util::setMockMode(true);

    auto& conf = faabric::util::getSystemConfig();
    std::string thisHost = conf.endpointHost;

    // Add other host to available hosts
    faabric::scheduler::Scheduler& sch = faabric::scheduler::getScheduler();
    std::string otherHost = "other";
    sch.addHostToGlobalSet(otherHost);

    // Make sure we have only enough resources to execute the initial function
    faabric::HostResources res;
    res.set_slots(1);
    sch.setThisHostResources(res);

    // Set up other host to have some resources
    faabric::HostResources resOther;
    resOther.set_slots(10);
    faabric::scheduler::queueResourceResponse(otherHost, resOther);

    // Background thread to execute main function and await results
    std::thread t([] {
        int nThreads = 8;
        std::shared_ptr<BatchExecuteRequest> req =
          faabric::util::batchExecFactory("dummy", "thread-check", 1);
        faabric::Message& msg = req->mutable_messages()->at(0);
        msg.set_inputdata(std::to_string(nThreads));

        auto& sch = faabric::scheduler::getScheduler();
        sch.callFunctions(req, false);

        faabric::Message res = sch.getFunctionResult(msg.id(), 2000);
        REQUIRE(res.returnvalue() == 0);
    });

    // Give it time to have made the request
    usleep(SHORT_TEST_TIMEOUT_MS * 500);

    // Check restore hasn't been called yet
    REQUIRE(restoreCount == 0);

    // Get the request that's been submitted
    auto reqs = faabric::scheduler::getBatchRequests();
    REQUIRE(reqs.size() == 1);
    std::string actualHost = reqs.at(0).first;
    REQUIRE(actualHost == otherHost);

    // Check the snapshot has been pushed to the other host
    auto snapPushes = faabric::scheduler::getSnapshotPushes();
    REQUIRE(snapPushes.size() == 1);
    REQUIRE(snapPushes.at(0).first == otherHost);

    std::shared_ptr<faabric::BatchExecuteRequest> distReq = reqs.at(0).second;
    faabric::Message firstMsg = distReq->messages().at(0);

    // Now execute request on this host as if we were on the other host
    conf.endpointHost = otherHost;
    sch.callFunctions(distReq, true);
    conf.endpointHost = thisHost;

    // Check restore has been called as we're not on master
    REQUIRE(restoreCount == 1);

    // Process the thread result requests
    std::vector<std::pair<std::string, faabric::ThreadResultRequest>> results =
      faabric::scheduler::getThreadResults();

    for (auto& r : results) {
        REQUIRE(r.first == thisHost);
        sch.setThreadResult(r.second.messageid(), r.second.returnvalue());
    }

    // Rejoin the other thread
    if (t.joinable()) {
        t.join();
    }

    // Shutdown
    sch.shutdown();
}

TEST_CASE("Test thread results returned on non-master", "[executor]")
{
    cleanFaabric();

    faabric::util::setMockMode(true);
    std::string otherHost = "other";

    int nThreads = 5;
    std::shared_ptr<BatchExecuteRequest> req =
      faabric::util::batchExecFactory("dummy", "blah", nThreads);
    req->set_type(faabric::BatchExecuteRequest::THREADS);

    std::vector<uint32_t> messageIds;
    std::string snapKey = setUpDummySnapshot();
    for (int i = 0; i < nThreads; i++) {
        faabric::Message& msg = req->mutable_messages()->at(i);
        msg.set_snapshotkey(snapKey);
        msg.set_masterhost(otherHost);

        messageIds.emplace_back(msg.id());
    }

    executeWithTestExecutor(req, true);

    // We have to manually add a wait here as the thread results won't actually
    // get logged on this host
    usleep(SHORT_TEST_TIMEOUT_MS * 1000);

    std::vector<std::pair<std::string, faabric::ThreadResultRequest>> actual =
      faabric::scheduler::getThreadResults();
    REQUIRE(actual.size() == nThreads);

    std::vector<uint32_t> actualMessageIds;
    for (auto& p : actual) {
        REQUIRE(p.first == otherHost);
        REQUIRE(p.second.returnvalue() == p.second.messageid() / 100);

        actualMessageIds.push_back(p.second.messageid());
    }

    std::sort(actualMessageIds.begin(), actualMessageIds.end());
    std::sort(messageIds.begin(), messageIds.end());

    REQUIRE(actualMessageIds == messageIds);

    faabric::util::setMockMode(false);

    faabric::scheduler::getScheduler().shutdown();
}

TEST_CASE("Test non-zero return code", "[executor]")
{
    cleanFaabric();

    std::shared_ptr<BatchExecuteRequest> req =
      faabric::util::batchExecFactory("dummy", "ret-one", 1);
    faabric::Message& msg = req->mutable_messages()->at(0);

    executeWithTestExecutor(req, false);

    auto& sch = faabric::scheduler::getScheduler();
    faabric::Message res = sch.getFunctionResult(msg.id(), 2000);
    REQUIRE(res.returnvalue() == 1);

    sch.shutdown();
}

TEST_CASE("Test erroring function", "[executor]")
{
    cleanFaabric();

    std::shared_ptr<BatchExecuteRequest> req =
      faabric::util::batchExecFactory("dummy", "error", 1);
    faabric::Message& msg = req->mutable_messages()->at(0);

    executeWithTestExecutor(req, false);

    auto& sch = faabric::scheduler::getScheduler();
    faabric::Message res = sch.getFunctionResult(msg.id(), 2000);
    REQUIRE(res.returnvalue() == 1);

    std::string expectedErrorMsg = fmt::format(
      "Task {} threw exception. What: This is a test error", msg.id());
    REQUIRE(res.outputdata() == expectedErrorMsg);

    sch.shutdown();
}

TEST_CASE("Test erroring thread", "[executor]")
{
    cleanFaabric();

    std::shared_ptr<BatchExecuteRequest> req =
      faabric::util::batchExecFactory("dummy", "error", 1);
    faabric::Message& msg = req->mutable_messages()->at(0);
    req->set_type(faabric::BatchExecuteRequest::THREADS);

    std::string snapKey = setUpDummySnapshot();
    msg.set_snapshotkey(snapKey);
    executeWithTestExecutor(req, false);

    auto& sch = faabric::scheduler::getScheduler();
    int32_t res = sch.awaitThreadResult(msg.id());
    REQUIRE(res == 1);

    sch.shutdown();
}

TEST_CASE("Test executing different functions", "[executor]")
{
    cleanFaabric();

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

    auto& conf = faabric::util::getSystemConfig();
    int boundOriginal = conf.boundTimeout;
    int overrideCpuOriginal = conf.overrideCpuCount;

    conf.overrideCpuCount = 10;
    conf.boundTimeout = SHORT_TEST_TIMEOUT_MS;

    // Execute all the functions
    auto& sch = faabric::scheduler::getScheduler();
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

    conf.boundTimeout = boundOriginal;
    conf.overrideCpuCount = overrideCpuOriginal;

    sch.shutdown();
}

TEST_CASE("Test claiming and releasing executor", "[executor]")
{
    cleanFaabric();

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

}
