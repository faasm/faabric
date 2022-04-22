#include "faabric_utils.h"
#include <catch2/catch.hpp>

#include "fixtures.h"
#include "init.h"

#include <faabric/scheduler/Scheduler.h>

namespace tests {

/*
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

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI checks", "[.]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("checks");

    // Call the functions
    sch.callFunctions(req);

    checkAllocationAndResult(req);
}
*/

// TODO - there's something wrong with gather, check implementation against faasm
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

/*
TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI async. send recv", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("isendrecv");

    // Call the functions
    sch.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI one sided comms.", "[.]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("onesided");

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

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI probe", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("probe");

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

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI window creation", "[.]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("win-create");

    // Call the functions
    sch.callFunctions(req);

    checkAllocationAndResult(req);
}
*/
}
