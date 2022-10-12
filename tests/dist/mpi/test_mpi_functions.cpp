#include "faabric_utils.h"
#include <catch2/catch.hpp>

#include "fixtures.h"
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

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI all to all", "[.][mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("alltoall");

    // Call the functions
    sch.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture,
                 "Test MPI all to all many times",
                 "[.][mpi]")
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

TEST_CASE_METHOD(MpiDistTestsFixture,
                 "Test MPI all to all and sleep",
                 "[.][mpi]")
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
    setLocalSlots(nLocalSlots);
    auto req = setRequest("cart-create");

    // Call the functions
    sch.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI cartesian", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
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
    // We fist distribute the execution, and update the local slots
    // mid-execution to fit all ranks, and create a migration opportunity.
    int localSlots = 2;
    int worldSize = 4;
    setLocalSlots(localSlots, worldSize);

    auto req = setRequest("migration");
    auto& msg = req->mutable_messages()->at(0);

    // Check very often for migration opportunities so that we detect it
    // right away
    msg.set_migrationcheckperiod(1);
    msg.set_inputdata(std::to_string(NUM_MIGRATION_LOOPS));

    // Call the functions
    sch.callFunctions(req);

    // Sleep for a while to let the scheduler schedule the MPI calls
    SLEEP_MS(500);

    // Update the local slots so that a migration opportunity appears
    int newLocalSlots = worldSize;
    setLocalSlots(newLocalSlots, worldSize);

    // The current function migration approach breaks the execution graph, as
    // some messages are left dangling (deliberately) without return value
    std::vector<std::string> hostsBeforeMigration = {
        getMasterIP(), getMasterIP(), getWorkerIP(), getWorkerIP()
    };
    std::vector<std::string> hostsAfterMigration(worldSize, getMasterIP());
    checkAllocationAndResultMigration(
      req, hostsBeforeMigration, hostsAfterMigration, 15000);
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
