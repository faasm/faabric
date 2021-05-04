#include "faabric_utils.h"
#include <catch.hpp>

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/ExecutorFactory.h>
#include <faabric/scheduler/FunctionCallClient.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/util/config.h>
#include <faabric/util/func.h>
#include <faabric/util/testing.h>

using namespace faabric::scheduler;

namespace tests {

std::atomic<int> restoreCount = 0;

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
                        std::shared_ptr<faabric::BatchExecuteRequest> req)
    {
        auto logger = faabric::util::getLogger();
        faabric::Message& msg = req->mutable_messages()->at(msgIdx);
        bool isThread = req->type() == faabric::BatchExecuteRequest::THREADS;

        // Custom thread-check function
        if (msg.function() == "thread-check" && !isThread) {
            msg.set_outputdata(fmt::format(
              "Threaded function {} executed successfully", msg.id()));

            // Set up the request
            int nThreads = 5;
            if (!msg.inputdata().empty()) {
                nThreads = std::stoi(msg.inputdata());
            }

            std::shared_ptr<faabric::BatchExecuteRequest> req =
              faabric::util::batchExecFactory(
                "dummy", "thread-check", nThreads);
            req->set_type(faabric::BatchExecuteRequest::THREADS);

            for (int i = 0; i < req->messages_size(); i++) {
                faabric::Message& m = req->mutable_messages()->at(i);
                m.set_snapshotkey(msg.snapshotkey());
                m.set_appindex(i + 1);
            }

            // Call the threads
            Scheduler& sch = getScheduler();
            sch.callFunctions(req);

            for (auto& m : req->messages()) {
                sch.awaitThreadResult(m.id());
            }
        } else if (msg.function() == "ret-one") {
            return 1;
        } else if (msg.function() == "error") {
            throw std::runtime_error("This is a test error");
        } else if (req->type() == faabric::BatchExecuteRequest::THREADS) {
            auto logger = faabric::util::getLogger();
            logger->debug("TestExecutor executing thread {}", msg.id());

            return msg.id() / 100;
        } else {
            msg.set_outputdata(fmt::format(
              "Simple function {} executed successfully", msg.id()));
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
    std::string expected =
      fmt::format("Simple function {} executed successfully", msgId);
    REQUIRE(result.outputdata() == expected);

    // Check that restore has not been called
    REQUIRE(restoreCount == 0);
}

TEST_CASE("Test executing threads directly", "[executor]")
{
    cleanFaabric();
    restoreCount = 0;

    int nThreads = 10;
    std::shared_ptr<BatchExecuteRequest> req =
      faabric::util::batchExecFactory("dummy", "blah", nThreads);
    req->set_type(faabric::BatchExecuteRequest::THREADS);

    std::vector<uint32_t> messageIds;
    std::string snapKey = setUpDummySnapshot();
    for (int i = 0; i < nThreads; i++) {
        req->mutable_messages()->at(i).set_snapshotkey(snapKey);
        messageIds.emplace_back(req->messages().at(i).id());
    }

    executeWithTestExecutor(req, false);

    auto& sch = faabric::scheduler::getScheduler();
    for (int i = 0; i < nThreads; i++) {
        uint32_t msgId = messageIds.at(i);
        int32_t result = sch.awaitThreadResult(msgId);
        REQUIRE(result == msgId / 100);
    }

    // Check that restore has only been called once
    REQUIRE(restoreCount == 1);
}

TEST_CASE("Test executing threads indirectly", "[executor]")
{
    cleanFaabric();
    restoreCount = 0;

    int nThreads = 8;
    std::shared_ptr<BatchExecuteRequest> req =
      faabric::util::batchExecFactory("dummy", "thread-check", 1);
    faabric::Message& msg = req->mutable_messages()->at(0);
    msg.set_inputdata(std::to_string(nThreads));

    std::string snapKey = setUpDummySnapshot();
    std::vector<uint32_t> messageIds;
    msg.set_snapshotkey(snapKey);

    executeWithTestExecutor(req, false);

    auto& sch = faabric::scheduler::getScheduler();
    faabric::Message res = sch.getFunctionResult(msg.id(), 2000);
    REQUIRE(res.returnvalue() == 0);

    // Check that restore has only been called once
    REQUIRE(restoreCount == 1);
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
}
}
