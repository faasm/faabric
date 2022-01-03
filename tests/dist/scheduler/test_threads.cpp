#include <catch2/catch.hpp>

#include "faabric_utils.h"
#include "fixtures.h"
#include "init.h"

#include <sys/mman.h>

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/snapshot/SnapshotClient.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/util/config.h>
#include <faabric/util/func.h>
#include <faabric/util/gids.h>
#include <faabric/util/snapshot.h>

namespace tests {

TEST_CASE_METHOD(DistTestsFixture,
                 "Test executing threads on multiple hosts",
                 "[snapshots][threads]")
{
    // Set up this host's resources
    int nLocalSlots = 2;
    int nThreads = 4;
    faabric::HostResources res;
    res.set_slots(nLocalSlots);
    sch.setThisHostResources(res);

    // Set up the message
    std::shared_ptr<faabric::BatchExecuteRequest> req =
      faabric::util::batchExecFactory("threads", "simple", nThreads);
    req->set_type(faabric::BatchExecuteRequest::THREADS);

    for (int i = 0; i < nThreads; i++) {
        faabric::Message& m = req->mutable_messages()->at(i);
        m.set_appidx(i);
    }

    // Set up main thread snapshot
    faabric::snapshot::SnapshotRegistry& reg =
      faabric::snapshot::getSnapshotRegistry();
    faabric::Message& msg = req->mutable_messages()->at(0);

    std::string snapshotKey = faabric::util::getMainThreadSnapshotKey(msg);
    auto snap = std::make_shared<faabric::util::SnapshotData>(
      DIST_TEST_EXECUTOR_MEMORY_SIZE);
    reg.registerSnapshot(snapshotKey, snap);

    // Call the functions
    sch.callFunctions(req);

    // Check threads executed on this host
    for (int i = 0; i < nLocalSlots; i++) {
        faabric::Message& m = req->mutable_messages()->at(i);
        int res = sch.awaitThreadResult(m.id());
        REQUIRE(res == m.id() / 2);
    }

    // Check threads executed on the other host
    for (int i = nLocalSlots; i < nThreads; i++) {
        faabric::Message& m = req->mutable_messages()->at(i);
        int res = sch.awaitThreadResult(m.id());
        REQUIRE(res == m.id() / 2);
    }
}
}
