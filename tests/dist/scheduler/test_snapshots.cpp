#include "faabric_utils.h"
#include <catch.hpp>

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
#include <faabric/util/snapshot.h>

namespace tests {

TEST_CASE_METHOD(DistTestsFixture,
                 "Check snapshots sent back from worker are applied",
                 "[snapshots]")
{
    std::string user = "snapshots";
    std::string function = "fake-diffs";
    std::string snapshotKey = "dist-snap-check";

    size_t snapSize = 2 * faabric::util::HOST_PAGE_SIZE;
    uint8_t* snapMemory = (uint8_t*)mmap(
      nullptr, snapSize, PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);

    faabric::util::SnapshotData snap;
    snap.data = snapMemory;
    snap.size = snapSize;

    reg.takeSnapshot(snapshotKey, snap);

    // Invoke the function that ought to send back some snapshot diffs that
    // should be applied
    std::shared_ptr<faabric::BatchExecuteRequest> req =
      faabric::util::batchExecFactory(user, function, 1);
    req->set_type(faabric::BatchExecuteRequest::THREADS);

    // Set up some input data
    faabric::Message& m = req->mutable_messages()->at(0);
    std::vector<uint8_t> inputData = { 0, 1, 2, 3, 4, 5, 6 };
    m.set_inputdata(inputData.data(), inputData.size());
    m.set_snapshotkey(snapshotKey);

    // Force the function to be executed remotely
    faabric::HostResources res;
    res.set_slots(0);
    sch.setThisHostResources(res);

    std::vector<std::string> expectedHosts = { WORKER_IP };
    std::vector<std::string> executedHosts = sch.callFunctions(req);
    REQUIRE(expectedHosts == executedHosts);

    int actualResult = sch.awaitThreadResult(m.id());
    REQUIRE(actualResult == 123);

    // Check the diffs have been applied
    size_t sizeA = snapshotKey.size();
    size_t sizeB = inputData.size();
    uint8_t* startA = snapMemory + 10;
    uint8_t* startB = snapMemory + 100;
    std::vector<uint8_t> actualA(startA, startA + sizeA);
    std::vector<uint8_t> actualB(startB, startB + sizeB);

    std::vector<uint8_t> expectedA = faabric::util::stringToBytes(snapshotKey);
    std::vector<uint8_t> expectedB = inputData;

    REQUIRE(actualA == expectedA);
    REQUIRE(actualB == expectedB);
}

TEST_CASE_METHOD(DistTestsFixture,
                 "Check snapshots sent back from child threads",
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

    std::vector<std::string> expectedHosts = { MASTER_IP };
    std::vector<std::string> executedHosts = sch.callFunctions(req);
    REQUIRE(expectedHosts == executedHosts);

    int actualResult = sch.awaitThreadResult(m.id());
    REQUIRE(actualResult == 333);
}
}
