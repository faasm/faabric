#include <catch2/catch.hpp>

#include "dist_test_fixtures.h"
#include "faabric_utils.h"
#include "init.h"
#include "mpi/mpi_native.h"

#include <faabric/scheduler/Scheduler.h>

namespace tests {

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI all gather", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("allgather");

    // Call the functions
    sch.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI all reduce", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("allreduce");

    // Call the functions
    sch.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI all to all", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("alltoall");

    // Call the functions
    auto decision = sch.callFunctions(req);

    checkAllocationAndResult(req);
}

/* TODO: flaky - FIXME
TEST_CASE_METHOD(MpiDistTestsFixture,
                 "Test MPI all to all many times",
                 "[mpi]")
{
    int numRuns = 50;
    int oldNumLocalSlots = nLocalSlots;
    nLocalSlots = 4;
    int worldSize = 8;
    for (int i = 0; i < numRuns; i++) {
        SPDLOG_DEBUG("Starting run {}/{}", i + 1, numRuns);
        // Set up this host's resources
        setLocalSlots(nLocalSlots, worldSize);
        auto req = setRequest("alltoall");

        // Call the functions
        sch.callFunctions(req);

        checkAllocationAndResult(req);
    }

    nLocalSlots = oldNumLocalSlots;
}
*/

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI all to all and sleep", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("alltoall-sleep");

    // Call the functions
    sch.callFunctions(req);

    // Wait for extra time as the test will sleep for five seconds
    checkAllocationAndResult(req, 20000);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI barrier", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("barrier");

    // Call the functions
    sch.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI broadcast", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("bcast");

    // Call the functions
    sch.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI cart create", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(8, 4);
    auto req = setRequest("cart-create");

    // Call the functions
    sch.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI cartesian", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(8, 4);
    auto req = setRequest("cartesian");

    // Call the functions
    sch.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI checks", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("checks");

    // Call the functions
    sch.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI function migration", "[mpi]")
{
    // We first set the slots so that the first four ranks (including the main
    // rank) are scheduled to the local world, and 4 more to the remote world
    int localSlots = 4;
    int worldSize = 8;
    setLocalSlots(localSlots, worldSize);

    auto req = setRequest("migration");
    auto& msg = req->mutable_messages()->at(0);
    msg.set_inputdata(std::to_string(NUM_MIGRATION_LOOPS));

    // Call the functions
    sch.callFunctions(req);

    // Sleep for a while to let the scheduler schedule the MPI calls
    SLEEP_MS(500);

    auto decision1 = sch.getPlannerClient()->getSchedulingDecision(req);

    // Update the slots so that a migration opportunity appears. We update
    // either the local or remote worlds to force the migration of one
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

    std::vector<std::string> hostsBeforeMigration = {
        getMasterIP(), getMasterIP(), getMasterIP(), getMasterIP(),
        getWorkerIP(), getWorkerIP(), getWorkerIP(), getWorkerIP()
    };
    REQUIRE(decision1.hosts == hostsBeforeMigration);

    std::vector<std::string> hostsAfterMigration;
    if (migratingMainRank) {
        hostsAfterMigration = { getWorkerIP(), getWorkerIP(), getWorkerIP(),
                                getWorkerIP(), getWorkerIP(), getWorkerIP(),
                                getWorkerIP(), getWorkerIP() };
    } else {
        hostsAfterMigration = { getMasterIP(), getMasterIP(), getMasterIP(),
                                getMasterIP(), getMasterIP(), getMasterIP(),
                                getMasterIP(), getMasterIP() };
    }
    checkAllocationAndResult(req, 15000, hostsAfterMigration);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI gather", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("gather");

    // Call the functions
    sch.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI hello world", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("hello-world");

    // Call the functions
    sch.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI async. send recv", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("isendrecv");

    // Call the functions
    sch.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI order", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("order");

    // Call the functions
    sch.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI reduce", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("reduce");

    // Call the functions
    sch.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI reduce many times", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("reduce-many");

    // Call the functions
    sch.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI scan", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("scan");

    // Call the functions
    sch.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI scatter", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("scatter");

    // Call the functions
    sch.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI send", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("send");

    // Call the functions
    sch.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture,
                 "Test sending sync and async messages",
                 "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("send-sync-async");

    // Call the functions
    sch.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test sending many MPI messages", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("send-many");

    // Call the functions
    sch.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI send-recv", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("sendrecv");

    // Call the functions
    sch.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI status", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("status");

    // Call the functions
    sch.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI types sizes", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("typesize");

    // Call the functions
    sch.callFunctions(req);

    checkAllocationAndResult(req);
}
}
