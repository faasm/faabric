#include <catch2/catch.hpp>

#include "dist_test_fixtures.h"
#include "faabric_utils.h"
#include "init.h"
#include "mpi/mpi_native.h"

#include <faabric/scheduler/Scheduler.h>

namespace tests {

TEST_CASE_METHOD(MpiDistTestsFixture,
                 "Test concurrent MPI applications with same master host",
                 "[mpi]")
{
    // Prepare both requests
    auto req1 = setRequest("alltoall-sleep");
    auto req2 = setRequest("alltoall-sleep");

    int worldSize = 8;

    // The first request will schedule two functions on each host
    setLocalSlots(4, worldSize);
    setRemoteSlots(4);
    sch.callFunctions(req1);

    // Sleep for a bit to allow for the scheduler to schedule all MPI ranks
    SLEEP_MS(200);

    // Override the local slots so that the same scheduling decision as before
    // is taken
    setLocalSlots(4, worldSize);
    setRemoteSlots(4);
    sch.callFunctions(req2);

    checkAllocationAndResult(req1, 15000);
    checkAllocationAndResult(req2, 15000);
}

TEST_CASE_METHOD(MpiDistTestsFixture,
                 "Test concurrent MPI applications with different master host",
                 "[mpi]")
{
    // Prepare the first request (local): 4 ranks locally, 4 remotely
    int worldSize = 8;
    setLocalSlots(4, worldSize);
    auto req1 = setRequest("alltoall-sleep");
    sch.callFunctions(req1);

    // Sleep for a bit to allow for the scheduler to schedule all MPI ranks
    SLEEP_MS(1000);

    // Prepare the second request (remote): 4 ranks remotely, 2 locally
    int newWorldSize = 6;
    setLocalSlots(2, newWorldSize);
    setRemoteSlots(4);
    auto req2 = setRequest("alltoall-sleep");
    sch.callFunctions(req2);

    std::vector<std::string> expectedHosts1 = { getMasterIP(), getMasterIP(),
                                                getMasterIP(), getMasterIP(),
                                                getWorkerIP(), getWorkerIP(),
                                                getWorkerIP(), getWorkerIP() };
    checkAllocationAndResult(req1, 15000, expectedHosts1);
    std::vector<std::string> hostsAfterMigration(worldSize, getMasterIP());
    std::vector<std::string> expectedHosts2 = { getWorkerIP(), getWorkerIP(),
                                                getWorkerIP(), getWorkerIP(),
                                                getMasterIP(), getMasterIP() };
    checkAllocationAndResult(req2, 15000, expectedHosts2);
}

TEST_CASE_METHOD(MpiDistTestsFixture,
                 "Test MPI migration with two MPI worlds",
                 "[mpi]")
{
    // Set the slots for the first request: 4 locally and 4 remote
    int worldSize = 8;
    setLocalSlots(4, worldSize);

    // Prepare both requests:
    // - The first will do work, sleep for five seconds, and do work again
    // - The second will do work and check for migration opportunities
    auto req1 = setRequest("alltoall-sleep");
    auto req2 = setRequest("migration");
    auto& msg = req2->mutable_messages()->at(0);
    msg.set_inputdata(std::to_string(NUM_MIGRATION_LOOPS));

    // The first request will schedule four functions on each host
    sch.callFunctions(req1);

    // Sleep for a while
    SLEEP_MS(5000);

    // Update the local and remote slots so that the second request is also
    // split among two worlds
    setLocalSlots(4, worldSize);
    setRemoteSlots(4);
    sch.callFunctions(req2);

    SLEEP_MS(200);
    auto decisionBefore = sch.getPlannerClient()->getSchedulingDecision(req2);

    // Update the slots again so that a migration opportunity appears. We
    // update either the local or remote worlds to force the migration of one
    // half of the ranks or the other one
    bool migratingMainRank;

    SECTION("Migrate main rank")
    {
        // Make more space remotely, so we migrate the first half of ranks
        // (including the main rank)
        migratingMainRank = true;
        setRemoteSlots(worldSize);
    }

    SECTION("Don't migrate main rank")
    {
        // Make more space locally, so we migrate the second half of ranks
        migratingMainRank = false;
        setLocalSlots(worldSize, worldSize);
    }

    std::vector<std::string> expectedHosts1 = { getMasterIP(), getMasterIP(),
                                                getMasterIP(), getMasterIP(),
                                                getWorkerIP(), getWorkerIP(),
                                                getWorkerIP(), getWorkerIP() };
    checkAllocationAndResult(req1, 15000, expectedHosts1);

    // Check hosts before migration
    std::vector<std::string> expectedHostsBeforeMigration = {
        getMasterIP(), getMasterIP(), getMasterIP(), getMasterIP(),
        getWorkerIP(), getWorkerIP(), getWorkerIP(), getWorkerIP()
    };
    REQUIRE(decisionBefore.hosts == expectedHostsBeforeMigration);

    std::vector<std::string> expectedHostsAfterMigration;
    if (migratingMainRank) {
        expectedHostsAfterMigration = { getWorkerIP(), getWorkerIP(),
                                        getWorkerIP(), getWorkerIP(),
                                        getWorkerIP(), getWorkerIP(),
                                        getWorkerIP(), getWorkerIP() };
    } else {
        expectedHostsAfterMigration = { getMasterIP(), getMasterIP(),
                                        getMasterIP(), getMasterIP(),
                                        getMasterIP(), getMasterIP(),
                                        getMasterIP(), getMasterIP() };
    }
    checkAllocationAndResult(req2, 15000, expectedHostsAfterMigration);
}
}
