#include "faabric/proto/faabric.pb.h"
#include "faabric/scheduler/Scheduler.h"
#include "faabric/snapshot/SnapshotRegistry.h"
#include "faabric/util/config.h"
#include "faabric/util/func.h"
#include "faabric_utils.h"
#include <catch.hpp>

#include <faabric/executor/DummyExecutorPool.h>

using namespace faabric::executor;

namespace tests {

void executeWithDummyExecutor(std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    auto& conf = faabric::util::getSystemConfig();
    int boundOriginal = conf.boundTimeout;
    int unboundOriginal = conf.unboundTimeout;
    int threadPoolOriginal = conf.executorThreadPoolSize;

    conf.executorThreadPoolSize = 10;
    conf.boundTimeout = 1000;
    conf.unboundTimeout = 1000;

    DummyExecutorPool pool(4);
    pool.startThreadPool(true);

    auto& sch = faabric::scheduler::getScheduler();
    sch.callFunctions(req);

    // Pool will wait for executors before shutting down
    pool.shutdown();

    conf.boundTimeout = boundOriginal;
    conf.unboundTimeout = unboundOriginal;
    conf.executorThreadPoolSize = threadPoolOriginal;
}

TEST_CASE("Test executing simple function", "[executor]")
{
    cleanFaabric();

    std::shared_ptr<BatchExecuteRequest> req =
      faabric::util::batchExecFactory("foo", "bar", 1);
    uint32_t msgId = req->messages().at(0).id();

    REQUIRE(req->messages_size() == 1);

    executeWithDummyExecutor(req);

    auto& sch = faabric::scheduler::getScheduler();
    faabric::Message result = sch.getFunctionResult(msgId, 1000);
    std::string expected =
      fmt::format("Function {} executed successfully", msgId);
    REQUIRE(result.outputdata() == expected);
}

TEST_CASE("Test executing threads", "[executor]")
{
    cleanFaabric();

    int nThreads = 10;
    std::shared_ptr<BatchExecuteRequest> req =
      faabric::util::batchExecFactory("foo", "bar", nThreads);
    req->set_type(faabric::BatchExecuteRequest::THREADS);

    // Set up a snapshot
    std::vector<uint8_t> snapData = { 0, 1, 2, 3, 4 };
    faabric::snapshot::SnapshotRegistry& reg =
      faabric::snapshot::getSnapshotRegistry();
    faabric::util::SnapshotData snap;
    std::string snapKey = "foobar";
    snap.data = snapData.data();
    snap.size = snapData.size();

    reg.takeSnapshot(snapKey, snap);

    std::vector<uint32_t> messageIds;
    for (int i = 0; i < nThreads; i++) {
        req->mutable_messages()->at(i).set_snapshotkey(snapKey);
        messageIds.emplace_back(req->messages().at(i).id());
    }

    executeWithDummyExecutor(req);

    auto& sch = faabric::scheduler::getScheduler();
    for (int i = 0; i < nThreads; i++) {
        uint32_t msgId = messageIds.at(i);
        faabric::Message result = sch.getFunctionResult(msgId, 2000);
        std::string expected =
          fmt::format("Thread {} executed successfully", msgId);
        REQUIRE(result.outputdata() == expected);
    }
}
}
