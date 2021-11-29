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
}
