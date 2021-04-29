#include "faabric_utils.h"
#include <catch.hpp>

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/util/config.h>
#include <faabric/util/func.h>

namespace tests {

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

void executeWithDummyExecutor(std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    auto& conf = faabric::util::getSystemConfig();
    int boundOriginal = conf.boundTimeout;
    int overrideCpuOriginal = conf.overrideCpuCount;

    conf.overrideCpuCount = 10;
    conf.boundTimeout = 1000;

    auto& sch = faabric::scheduler::getScheduler();
    sch.callFunctions(req);

    conf.boundTimeout = boundOriginal;
    conf.overrideCpuCount = overrideCpuOriginal;
}

TEST_CASE("Test executing simple function", "[executor]")
{
    cleanFaabric();

    std::shared_ptr<BatchExecuteRequest> req =
      faabric::util::batchExecFactory("dummy", "simple", 1);
    uint32_t msgId = req->messages().at(0).id();

    REQUIRE(req->messages_size() == 1);

    executeWithDummyExecutor(req);

    auto& sch = faabric::scheduler::getScheduler();
    faabric::Message result = sch.getFunctionResult(msgId, 1000);
    std::string expected =
      fmt::format("Simple function {} executed successfully", msgId);
    REQUIRE(result.outputdata() == expected);
}

TEST_CASE("Test executing threads directly", "[executor]")
{
    cleanFaabric();

    int nThreads = 10;
    std::shared_ptr<BatchExecuteRequest> req =
      faabric::util::batchExecFactory("dummy", "thread-check", nThreads);
    req->set_type(faabric::BatchExecuteRequest::THREADS);

    std::vector<uint32_t> messageIds;
    std::string snapKey = setUpDummySnapshot();
    for (int i = 0; i < nThreads; i++) {
        req->mutable_messages()->at(i).set_snapshotkey(snapKey);
        messageIds.emplace_back(req->messages().at(i).id());
    }

    executeWithDummyExecutor(req);

    auto& sch = faabric::scheduler::getScheduler();
    for (int i = 0; i < nThreads; i++) {
        uint32_t msgId = messageIds.at(i);
        int32_t result = sch.awaitThreadResult(msgId);
        REQUIRE(result == msgId / 100);
    }
}

TEST_CASE("Test executing threads indirectly", "[executor]")
{
    cleanFaabric();

    int nThreads = 8;
    std::shared_ptr<BatchExecuteRequest> req =
      faabric::util::batchExecFactory("dummy", "thread-check", 1);
    faabric::Message& msg = req->mutable_messages()->at(0);
    msg.set_inputdata(std::to_string(nThreads));

    std::string snapKey = setUpDummySnapshot();
    std::vector<uint32_t> messageIds;
    msg.set_snapshotkey(snapKey);

    executeWithDummyExecutor(req);

    auto& sch = faabric::scheduler::getScheduler();
    faabric::Message res = sch.getFunctionResult(msg.id(), 2000);
    REQUIRE(res.returnvalue() == 0);
}
}
