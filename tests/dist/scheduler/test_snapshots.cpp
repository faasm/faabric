#include <catch2/catch.hpp>

#include "faabric_utils.h"
#include "fixtures.h"
#include "init.h"

#include <sys/mman.h>

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/snapshot/SnapshotClient.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/util/bytes.h>
#include <faabric/util/config.h>
#include <faabric/util/func.h>
#include <faabric/util/memory.h>
#include <faabric/util/scheduling.h>
#include <faabric/util/snapshot.h>

namespace tests {

TEST_CASE_METHOD(DistTestsFixture,
                 "Check snapshots sent back from worker are queued",
                 "[snapshots]")
{
    std::string user = "snapshots";
    std::string function = "fake-diffs";
    std::string snapshotKey = "dist-snap-check";
    std::vector<uint8_t> inputData = { 0, 1, 2, 3, 4, 5, 6 };

    // Set up snapshot
    size_t snapSize = 2 * faabric::util::HOST_PAGE_SIZE;
    auto snap = std::make_shared<faabric::util::SnapshotData>(snapSize);

    reg.registerSnapshot(snapshotKey, snap);

    // Set up the message
    std::shared_ptr<faabric::BatchExecuteRequest> req =
      faabric::util::batchExecFactory(user, function, 1);
    req->set_type(faabric::BatchExecuteRequest::THREADS);

    // Set up some input data
    faabric::Message& m = req->mutable_messages()->at(0);
    m.set_inputdata(inputData.data(), inputData.size());
    m.set_snapshotkey(snapshotKey);

    // Force the function to be executed remotely
    faabric::HostResources res;
    res.set_slots(0);
    sch.setThisHostResources(res);

    std::vector<std::string> expectedHosts = { getWorkerIP() };
    faabric::util::SchedulingDecision decision = sch.callFunctions(req);
    std::vector<std::string> executedHosts = decision.hosts;
    REQUIRE(expectedHosts == executedHosts);

    int actualResult = sch.awaitThreadResult(m.id());
    REQUIRE(actualResult == 123);

    // Write the diffs and check they've been applied
    REQUIRE(snap->getQueuedDiffsCount() == 2);
    snap->writeQueuedDiffs();

    size_t sizeA = snapshotKey.size();
    size_t sizeB = inputData.size();

    const uint8_t* startA = snap->getDataPtr() + 10;
    const uint8_t* startB = snap->getDataPtr() + 100;
    std::vector<uint8_t> actualA(startA, startA + sizeA);
    std::vector<uint8_t> actualB(startB, startB + sizeB);

    std::vector<uint8_t> expectedA = faabric::util::stringToBytes(snapshotKey);
    std::vector<uint8_t> expectedB = inputData;

    REQUIRE(actualA == expectedA);
    REQUIRE(actualB == expectedB);
}

TEST_CASE_METHOD(DistTestsFixture,
                 "Check snapshot diffs sent back from child threads",
                 "[snapshots]")
{
    std::string user = "snapshots";
    std::string function = "fake-diffs-threaded";
    int nThreads = 3;

    std::shared_ptr<faabric::BatchExecuteRequest> req =
      faabric::util::batchExecFactory(user, function, 1);
    faabric::Message& m = req->mutable_messages()->at(0);
    m.set_inputdata(std::to_string(nThreads));

    // Force the function itself to be executed on this host, but its child
    // threads on another host
    faabric::HostResources res;
    res.set_slots(1);
    sch.setThisHostResources(res);

    std::vector<std::string> expectedHosts = { getMasterIP() };
    faabric::util::SchedulingDecision decision = sch.callFunctions(req);
    std::vector<std::string> executedHosts = decision.hosts;
    REQUIRE(expectedHosts == executedHosts);

    faabric::Message actualResult = sch.getFunctionResult(m.id(), 10000);
    REQUIRE(actualResult.returnvalue() == 333);
}

TEST_CASE_METHOD(DistTestsFixture, "Check repeated reduction", "[snapshots]")
{
    std::string user = "snapshots";
    std::string function = "reduction";

    std::shared_ptr<faabric::BatchExecuteRequest> req =
      faabric::util::batchExecFactory(user, function, 1);
    faabric::Message& m = req->mutable_messages()->at(0);

    // Main function and one thread execute on this host, others on another
    faabric::HostResources res;
    res.set_slots(3);
    sch.setThisHostResources(res);

    std::vector<std::string> expectedHosts = { getMasterIP() };
    faabric::util::SchedulingDecision decision = sch.callFunctions(req);
    std::vector<std::string> executedHosts = decision.hosts;
    REQUIRE(expectedHosts == executedHosts);

    faabric::Message actualResult = sch.getFunctionResult(m.id(), 10000);
    REQUIRE(actualResult.returnvalue() == 0);
}
}
