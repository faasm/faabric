#include "faabric_utils.h"
#include <catch.hpp>

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

namespace tests {

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
    size_t snapshotSize = 5 * faabric::util::HOST_PAGE_SIZE;
    auto* snapshotData = (uint8_t*)mmap(
      nullptr, snapshotSize, PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);

    faabric::util::SnapshotData snap;
    snap.data = snapshotData;
    snap.size = snapshotSize;

    std::string snapKey = std::to_string(faabric::util::generateGid());
    reg.takeSnapshot(snapKey, snap);

    faabric::Message& firstMsg = req->mutable_messages()->at(0);
    firstMsg.set_snapshotkey(snapKey);

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

    munmap(snapshotData, snapshotSize);
}
}
