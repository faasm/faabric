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

TEST_CASE_METHOD(MpiDistTestsFixture,
                 "Test concurrent MPI applications with different master host",
                 "[mpi]")
{
    // Prepare the first request (local): 2 ranks locally, 2 remotely
    int worldSize = 4;
    setLocalSlots(2, worldSize);
    auto req1 = setRequest("alltoall-sleep");
    sch.callFunctions(req1);

    // Sleep for a bit to allow for the scheduler to schedule all MPI ranks
    SLEEP_MS(200);

    // Prepare the second request (remote): 4 ranks remotely, 2 locally
    int newWorldSize = 6;
    setLocalSlots(2, newWorldSize);
    auto req2 = setRequest("alltoall-sleep");
    // Request remote execution
    faabric::scheduler::FunctionCallClient cli(getWorkerIP());
    cli.executeFunctions(req2);

    // Skip the automated exec graph check, and check manually
    bool skipExecGraphCheck = true;
    checkAllocationAndResult(req1, 15000, skipExecGraphCheck);
    std::vector<std::string> hostsBeforeMigration = {
        getMasterIP(), getMasterIP(), getWorkerIP(), getWorkerIP()
    };
    std::vector<std::string> hostsAfterMigration(worldSize, getMasterIP());
    checkAllocationAndResult(req2, 15000, skipExecGraphCheck);

    // Check exec graph for first request
    auto execGraph1 =
      sch.getFunctionExecGraph(req1->mutable_messages()->at(0).id());
    std::vector<std::string> expectedHosts1 = {
        getMasterIP(), getMasterIP(), getWorkerIP(), getWorkerIP()
    };
    REQUIRE(expectedHosts1 ==
            faabric::scheduler::getMpiRankHostsFromExecGraph(execGraph1));

    // Check exec graph for second request
    auto execGraph2 =
      sch.getFunctionExecGraph(req2->mutable_messages()->at(0).id());
    std::vector<std::string> expectedHosts2 = { getWorkerIP(), getWorkerIP(),
                                                getMasterIP(), getMasterIP(),
                                                getWorkerIP(), getWorkerIP() };
    REQUIRE(expectedHosts2 ==
            faabric::scheduler::getMpiRankHostsFromExecGraph(execGraph2));
}

TEST_CASE_METHOD(MpiDistTestsFixture,
                 "Test MPI migration with two MPI worlds",
                 "[mpi]")
{
    // Set the slots for the first request
    int worldSize = 4;
    setLocalSlots(2, worldSize);

    // Prepare both requests
    auto req1 = setRequest("alltoall-sleep");
    auto req2 = setRequest("migration");
    auto& msg = req2->mutable_messages()->at(0);
    msg.set_migrationcheckperiod(5);
    msg.set_inputdata(std::to_string(NUM_MIGRATION_LOOPS));

    // The first request will schedule two functions on each host
    sch.callFunctions(req1);

    // Sleep for a bit to allow for the scheduler to schedule all MPI ranks
    SLEEP_MS(200);

    // Override the local slots so that when the first application finishes,
    // a migration opportunity appears
    setLocalSlots(2, worldSize);
    sch.callFunctions(req2);

    checkAllocationAndResult(req1, 15000);
    std::vector<std::string> hostsBeforeMigration = {
        getMasterIP(), getMasterIP(), getWorkerIP(), getWorkerIP()
    };
    std::vector<std::string> hostsAfterMigration(worldSize, getMasterIP());
    checkAllocationAndResultMigration(
      req2, hostsBeforeMigration, hostsAfterMigration, 15000);
}
}
