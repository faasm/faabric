#include <catch2/catch.hpp>

#include "dist_test_fixtures.h"
#include "faabric_utils.h"
#include "init.h"
#include "mpi/mpi_native.h"

#include <faabric/scheduler/Scheduler.h>

namespace tests {

TEST_CASE_METHOD(MpiDistTestsFixture,
                 "Test concurrent MPI applications with same main host",
                 "[mpi]")
{
    // Prepare both requests
    auto req1 = setRequest("alltoall-sleep");
    auto req2 = setRequest("alltoall-sleep");

    int worldSize = 4;

    // The first request will schedule two functions on each host
    setLocalSlots(2, worldSize);
    plannerCli.callFunctions(req1);

    // Sleep for a bit to allow for the scheduler to schedule all MPI ranks
    SLEEP_MS(400);

    // Override the local and remote slots so that the same scheduling
    // decision as before is taken (2 on each host). Given that both hosts
    // have the same number of slots, the planner will pick as main host the
    // one with more total slots
    bool sameMainHost;

    SECTION("Same main host")
    {
        sameMainHost = true;
        updateLocalSlots(2 * worldSize, 2 * worldSize - 2);
        updateRemoteSlots(worldSize, 2);
    }

    SECTION("Different main host")
    {
        sameMainHost = false;
        updateLocalSlots(worldSize, 2);
        updateRemoteSlots(2 * worldSize, 2 * worldSize - 2);
    }

    plannerCli.callFunctions(req2);

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
      faabric::util::getFunctionExecGraph(req1->mutable_messages()->at(0));
    std::vector<std::string> expectedHosts1 = {
        getMasterIP(), getMasterIP(), getWorkerIP(), getWorkerIP()
    };
    REQUIRE(expectedHosts1 ==
            faabric::util::getMpiRankHostsFromExecGraph(execGraph1));

    // Check exec graph for second request
    auto execGraph2 =
      faabric::util::getFunctionExecGraph(req2->mutable_messages()->at(0));
    std::vector<std::string> expectedHosts2;
    if (sameMainHost) {
        expectedHosts2 = expectedHosts1;
    } else {
        expectedHosts2 = {
            getWorkerIP(), getWorkerIP(), getMasterIP(), getMasterIP()
        };
    }
    REQUIRE(expectedHosts2 ==
            faabric::util::getMpiRankHostsFromExecGraph(execGraph2));
}

TEST_CASE_METHOD(MpiDistTestsFixture,
                 "Test MPI migration with two MPI worlds",
                 "[mpi]")
{
    // Set the slots for the first request: 2 locally and 2 remote
    int worldSize = 4;
    setLocalSlots(2, worldSize);

    // Prepare both requests:
    // - The first will do work, sleep for five seconds, and do work again
    // - The second will do work and check for migration opportunities
    auto req1 = setRequest("alltoall-sleep");
    auto req2 = setRequest("migration");
    auto& msg = req2->mutable_messages()->at(0);
    msg.set_inputdata(std::to_string(NUM_MIGRATION_LOOPS));

    // The first request will schedule two functions on each host
    plannerCli.callFunctions(req1);

    // Sleep for a while to let the application make some progress
    SLEEP_MS(1000);

    // Update the slots to make space for another app. Same scheduling as
    // the first one: two slots locally, two remotely
    updateLocalSlots(2 * worldSize, 2 * worldSize - 2);
    updateRemoteSlots(worldSize, 2);

    plannerCli.callFunctions(req2);

    // Sleep for a tiny bit for all MPI ranks to begin doing work
    SLEEP_MS(200);

    // Update the slots to create a migration opportunity. We migrate two ranks
    // from one host to the other, and we test migrating both ways
    bool migratingMainRank;

    SECTION("Migrate main rank")
    {
        // Make more space remotely, so we migrate the first half of ranks
        // (including the main rank)
        migratingMainRank = true;
        updateRemoteSlots(2 * worldSize, worldSize);
    }

    SECTION("Don't migrate main rank")
    {
        // Make more space locally, so we migrate the second half of ranks
        migratingMainRank = false;
        updateLocalSlots(2 * worldSize, worldSize);
    }

    checkAllocationAndResult(req1, 15000);
    std::vector<std::string> hostsBeforeMigration = {
        getMasterIP(), getMasterIP(), getWorkerIP(), getWorkerIP()
    };
    std::vector<std::string> hostsAfterMigration;
    if (migratingMainRank) {
        hostsAfterMigration = {
            getWorkerIP(), getWorkerIP(), getWorkerIP(), getWorkerIP()
        };
    } else {
        hostsAfterMigration = {
            getMasterIP(), getMasterIP(), getMasterIP(), getMasterIP()
        };
    }
    checkAllocationAndResultMigration(
      req2, hostsBeforeMigration, hostsAfterMigration, 15000);
}

TEST_CASE_METHOD(MpiDistTestsFixture,
                 "Test migrating two MPI applications in parallel",
                 "[mpi]")
{
    // Set the slots for the first request: 2 locally and 2 remote
    int worldSize = 4;
    setLocalSlots(2, worldSize);

    // Prepare both requests: both will do work and check for migration
    // opportunities
    auto req1 = setRequest("migration");
    req1->mutable_messages(0)->set_inputdata(
      std::to_string(NUM_MIGRATION_LOOPS));
    auto req2 = setRequest("migration");
    req2->mutable_messages(0)->set_inputdata(
      std::to_string(NUM_MIGRATION_LOOPS));

    // The first request will schedule two functions on each host
    plannerCli.callFunctions(req1);

    // Sleep for a tiny bit for all MPI ranks to begin doing work
    SLEEP_MS(100);

    // Update the slots to make space for another app. Same scheduling as
    // the first one: two slots locally, two remotely
    updateLocalSlots(2 * worldSize, 2 * worldSize - 2);
    updateRemoteSlots(worldSize, 2);
    plannerCli.callFunctions(req2);

    // Sleep for a tiny bit for all MPI ranks to begin doing work
    SLEEP_MS(100);

    // Update the slots to create two migration opportunities. For each app, we
    // migrate two ranks from one host to the other. The first app will
    // see that there are two free slots in the remote host (given the update
    // below), it will then migrate two ranks, freeing two other ranks in the
    // local world. The second app will see those newly freed slots, and use
    // them to migrate to.
    updateLocalSlots(3 * worldSize, 3 * worldSize);
    updateRemoteSlots(2 * worldSize, 2 * worldSize - 2);

    std::vector<std::string> hostsBeforeMigration1 = {
        getMasterIP(), getMasterIP(), getWorkerIP(), getWorkerIP()
    };
    std::vector<std::string> hostsAfterMigration1(worldSize, getWorkerIP());
    checkAllocationAndResultMigration(
      req1, hostsBeforeMigration1, hostsAfterMigration1, 15000);

    std::vector<std::string> hostsBeforeMigration2 = {
        getMasterIP(), getMasterIP(), getWorkerIP(), getWorkerIP()
    };
    std::vector<std::string> hostsAfterMigration2(worldSize, getMasterIP());
    checkAllocationAndResultMigration(
      req2, hostsBeforeMigration2, hostsAfterMigration2, 15000);
}
}
