#include "faabric_utils.h"
#include <catch.hpp>

#include "fixtures.h"
#include "init.h"

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/scheduler/SnapshotClient.h>
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

TEST_CASE_METHOD(DistTestsFixture,
                 "Test executing threads on multiple hosts",
                 "[threads]")
{
    // Set up this host's resources
    int nLocalSlots = 2;
    int nThreads = 4;
    faabric::HostResources res;
    res.set_slots(nLocalSlots);
    sch.setThisHostResources(res);

    // Set up the messages
    std::shared_ptr<faabric::BatchExecuteRequest> req =
      faabric::util::batchExecFactory("threads", "simple", 4);
    req->set_type(faabric::BatchExecuteRequest::THREADS);

    // Set up a snapshot
    std::string snapKey = setUpDummySnapshot();
    faabric::Message& firstMsg = req->mutable_messages()->at(0);
    firstMsg.set_snapshotkey(snapKey);

    // Call the functions
    sch.callFunctions(req);

    // Check threads executed on this host
    for (int i = 0; i < nLocalSlots; i++) {
        faabric::Message& m = req->mutable_messages()->at(i);
        int res = sch.awaitThreadResult(m.id());
        REQUIRE(res == 12 * m.id());
    }

    // Check threads executed on the other host
    for (int i = nLocalSlots; i < nThreads; i++) {
        faabric::Message& m = req->mutable_messages()->at(i);
        int res = sch.awaitThreadResult(m.id());
        REQUIRE(res == 12 * m.id());
    }
}
}
