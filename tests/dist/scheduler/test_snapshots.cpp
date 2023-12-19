#include <catch2/catch.hpp>

#include "dist_test_fixtures.h"
#include "faabric_utils.h"
#include "init.h"

#include <sys/mman.h>

#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/snapshot/SnapshotClient.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/util/bytes.h>
#include <faabric/util/config.h>
#include <faabric/util/func.h>
#include <faabric/util/memory.h>
#include <faabric/util/snapshot.h>

namespace tests {

TEST_CASE_METHOD(DistTestsFixture,
                 "Check snapshots sent back from worker are queued",
                 "[snapshots][threads]")
{
    std::string user = "snapshots";
    std::string function = "fake-diffs";
    std::vector<uint8_t> inputData = { 0, 1, 2, 3, 4, 5, 6 };

    // Set up the messages
    std::shared_ptr<faabric::BatchExecuteRequest> req =
      faabric::util::batchExecFactory(user, function, 2);
    req->set_type(faabric::BatchExecuteRequest::THREADS);

    for (int i = 0; i < req->messages_size(); i++) {
        req->mutable_messages(i)->set_groupidx(i);
        req->mutable_messages(i)->set_inputdata(inputData.data(),
                                                inputData.size());
    }

    // Set up the main thread snapshot
    auto& reg = faabric::snapshot::getSnapshotRegistry();
    size_t snapSize = DIST_TEST_EXECUTOR_MEMORY_SIZE;
    std::string snapshotKey =
      faabric::util::getMainThreadSnapshotKey(req->messages(0));
    auto snap = std::make_shared<faabric::util::SnapshotData>(snapSize);
    reg.registerSnapshot(snapshotKey, snap);

    // Force the execution to span multiple hosts so that it triggers dirty
    // tracking
    std::vector<std::string> expectedHosts = { getMasterIP(), getWorkerIP() };
    auto preloadDec = std::make_shared<batch_scheduler::SchedulingDecision>(
      req->appid(), req->groupid());
    for (int i = 0; i < req->messages_size(); i++) {
        preloadDec->addMessage(expectedHosts.at(i), 0, 0, i);
    }

    plannerCli.preloadSchedulingDecision(preloadDec);
    auto decision = plannerCli.callFunctions(req);
    REQUIRE(expectedHosts == decision.hosts);

    for (const auto& msg : req->messages()) {
        auto msgResult =
          plannerCli.getMessageResult(req->appid(), msg.id(), 500);
        REQUIRE(msgResult.returnvalue() == 123);
    }

    // Write the diffs and check they've been applied
    REQUIRE(snap->getQueuedDiffsCount() == 2);
    snap->writeQueuedDiffs();

    size_t expectedOffsetA = 10;
    size_t expectedOffsetB = faabric::util::HOST_PAGE_SIZE + 10;
    std::vector<uint8_t> expectedA = { 1, 2, 3, 4 };
    std::vector<uint8_t> expectedB = inputData;

    size_t sizeA = expectedA.size();
    size_t sizeB = expectedB.size();

    const uint8_t* startA = snap->getDataPtr() + expectedOffsetA;
    const uint8_t* startB = snap->getDataPtr() + expectedOffsetB;
    std::vector<uint8_t> actualA(startA, startA + sizeA);
    std::vector<uint8_t> actualB(startB, startB + sizeB);

    REQUIRE(actualA == expectedA);
    REQUIRE(actualB == expectedB);
}

TEST_CASE_METHOD(DistTestsFixture,
                 "Check snapshot diffs sent back from child threads",
                 "[snapshots][threads]")
{
    std::string user = "snapshots";
    std::string function = "fake-diffs-threaded";
    int nThreads = 3;

    std::shared_ptr<faabric::BatchExecuteRequest> req =
      faabric::util::batchExecFactory(user, function, 1);

    faabric::Message& msg = req->mutable_messages()->at(0);
    msg.set_inputdata(std::to_string(nThreads));

    // Executing one function that spawns three threads. We execute one locally
    // and the two others remotely
    faabric::HostResources res;
    res.set_slots(3);
    res.set_usedslots(1);
    sch.setThisHostResources(res);
    res.set_slots(2);
    res.set_usedslots(0);
    sch.addHostToGlobalSet(getWorkerIP(), std::make_shared<HostResources>(res));

    std::vector<std::string> expectedHosts = { getMasterIP() };
    auto decision = plannerCli.callFunctions(req);
    std::vector<std::string> executedHosts = decision.hosts;
    REQUIRE(expectedHosts == executedHosts);

    faabric::Message actualResult = plannerCli.getMessageResult(msg, 10000);
    REQUIRE(actualResult.returnvalue() == 333);
}

TEST_CASE_METHOD(DistTestsFixture,
                 "Check repeated reduction",
                 "[snapshots][threads]")
{
    std::string user = "snapshots";
    std::string function = "reduction";

    std::shared_ptr<faabric::BatchExecuteRequest> req =
      faabric::util::batchExecFactory(user, function, 1);
    faabric::Message& msg = req->mutable_messages()->at(0);

    // Main function will spawn 4 child threads in a loop. We execute two
    // locally and two remotely
    faabric::HostResources res;
    res.set_slots(3);
    res.set_usedslots(0);
    sch.setThisHostResources(res);
    res.set_usedslots(0);
    res.set_slots(2);
    sch.addHostToGlobalSet(getWorkerIP(), std::make_shared<HostResources>(res));

    std::vector<std::string> expectedHosts = { getMasterIP() };
    auto decision = plannerCli.callFunctions(req);
    std::vector<std::string> executedHosts = decision.hosts;
    REQUIRE(expectedHosts == executedHosts);

    faabric::Message actualResult = plannerCli.getMessageResult(msg, 10000);
    REQUIRE(actualResult.returnvalue() == 0);
}
}
