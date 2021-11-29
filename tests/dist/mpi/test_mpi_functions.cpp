#include "faabric_utils.h"
#include <catch2/catch.hpp>

#include "fixtures.h"
#include "init.h"

#include <faabric/scheduler/Scheduler.h>

namespace tests {

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI all gather", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("allgather", worldSize);

    // Call the functions
    sch.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI all reduce", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("allreduce", worldSize);

    // Call the functions
    sch.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI all to all", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("alltoall", worldSize);

    // Call the functions
    sch.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI broadcast", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("bcast", worldSize);

    // Call the functions
    sch.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI cart create", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("cart-create", worldSize);

    // Call the functions
    sch.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI cartesian", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("cartesian", worldSize);

    // Call the functions
    sch.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI checks", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("checks", worldSize);

    // Call the functions
    sch.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI gather", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("gather", worldSize);

    // Call the functions
    sch.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI hello world", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("hello-world", worldSize);

    // Call the functions
    sch.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI async. send recv", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("isendrecv", worldSize);

    // Call the functions
    sch.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI one sided comms.", "[.]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("onesided", worldSize);

    // Call the functions
    sch.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI order", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("order", worldSize);

    // Call the functions
    sch.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI probe", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("probe", worldSize);

    // Call the functions
    sch.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI reduce", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("reduce", worldSize);

    // Call the functions
    sch.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI scan", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("scan", worldSize);

    // Call the functions
    sch.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI scatter", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("scatter", worldSize);

    // Call the functions
    sch.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI send", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("send", worldSize);

    // Call the functions
    sch.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI send-recv", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("sendrecv", worldSize);

    // Call the functions
    sch.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI status", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("status", worldSize);

    // Call the functions
    sch.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI types sizes", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("typesize", worldSize);

    // Call the functions
    sch.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI window creation", "[.]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("win-create", worldSize);

    // Call the functions
    sch.callFunctions(req);

    checkAllocationAndResult(req);
}
}
