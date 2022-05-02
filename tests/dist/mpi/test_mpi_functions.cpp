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

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI all to all", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("alltoall");

    // Call the functions
    sch.callFunctions(req);

    checkAllocationAndResult(req);
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
    setLocalSlots(nLocalSlots);

    auto req = setRequest("migration");
    auto& msg = req->mutable_messages()->at(0);

    msg.set_migrationcheckperiod(5);
    msg.set_inputdata(std::to_string(NUM_MIGRATION_LOOPS));

    // Call the functions
    sch.callFunctions(req);

    // The current function migration approach breaks the execution graph, as
    // some messages are left dangling (deliberately) without return value
    bool skipExecGraphCheck = true;
    checkAllocationAndResult(req, 60000, skipExecGraphCheck);
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
