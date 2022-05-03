#include "faabric_utils.h"
#include <catch2/catch.hpp>

#include "fixtures.h"
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

    int worldSize = 4;

    // The first request will schedule two functions on each host
    setLocalSlots(2, worldSize);
    sch.callFunctions(req1);

    // Sleep for a bit to allow for the scheduler to schedule all MPI ranks
    SLEEP_MS(200);

    // Override the local slots so that the same scheduling decision as before
    // is taken
    setLocalSlots(2, worldSize);
    sch.callFunctions(req2);

    checkAllocationAndResult(req1, 15000);
    checkAllocationAndResult(req2, 15000);
}
}
