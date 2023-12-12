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
    plannerCli.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI all reduce", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("allreduce");

    // Call the functions
    plannerCli.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI all to all", "[.][mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("alltoall");

    // Call the functions
    plannerCli.callFunctions(req);

    checkAllocationAndResult(req);
}

/* 01/12/2023 - flaky tet
TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI all to all many times", "[mpi]")
{
    int numRuns = 5;
    int oldNumLocalSlots = nLocalSlots;
    nLocalSlots = 4;
    int worldSize = 8;
    for (int i = 0; i < numRuns; i++) {
        SPDLOG_DEBUG("Starting run {}/{}", i + 1, numRuns);
        // Set up this host's resources
        setLocalSlots(nLocalSlots, worldSize);
        auto req = setRequest("alltoall");

        // Call the functions
        plannerCli.callFunctions(req);

        checkAllocationAndResult(req);
    }

    nLocalSlots = oldNumLocalSlots;
}
*/

TEST_CASE_METHOD(MpiDistTestsFixture,
                 "Test MPI all to all and sleep",
                 "[.][mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("alltoall-sleep");

    // Call the functions
    plannerCli.callFunctions(req);

    // Wait for extra time as the test will sleep for five seconds
    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI barrier", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("barrier");

    // Call the functions
    plannerCli.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI broadcast", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("bcast");

    // Call the functions
    plannerCli.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI cart create", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("cart-create");

    // Call the functions
    plannerCli.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI cartesian", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("cartesian");

    // Call the functions
    plannerCli.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI checks", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("checks");

    // Call the functions
    plannerCli.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI function migration", "[mpi]")
{
    updateLocalSlots(worldSize);
    updateRemoteSlots(worldSize);

    auto req = setRequest("migration");
    auto& msg = req->mutable_messages()->at(0);
    msg.set_inputdata(std::to_string(NUM_MIGRATION_LOOPS));

    std::vector<std::string> hostsBeforeMigration;
    std::vector<std::string> hostsAfterMigration;

    SECTION("Migrate main rank")
    {
        hostsBeforeMigration = {
            getMasterIP(), getWorkerIP(), getWorkerIP(), getWorkerIP()
        };
        hostsAfterMigration = std::vector<std::string>(4, getWorkerIP());
    }

    SECTION("Don't migrate main rank")
    {
        hostsBeforeMigration = {
            getMasterIP(), getMasterIP(), getMasterIP(), getWorkerIP()
        };
        hostsAfterMigration = std::vector<std::string>(4, getMasterIP());
    }

    // Preload a scheduling decision to force a migration opportunity
    auto preloadDec = std::make_shared<batch_scheduler::SchedulingDecision>(
      req->appid(), req->groupid());
    for (int i = 0; i < worldSize; i++) {
        preloadDec->addMessage(hostsBeforeMigration.at(i), 0, 0, i);
    }

    // Preload decision
    plannerCli.preloadSchedulingDecision(preloadDec);

    // Call the functions
    plannerCli.callFunctions(req);

    auto actualHostsBeforeMigration = waitForMpiMessagesInFlight(req);
    REQUIRE(hostsBeforeMigration == actualHostsBeforeMigration);

    // Wait for messages to be finished
    checkAllocationAndResult(req, hostsAfterMigration);

    // Check that, indeed, one migration happened
    REQUIRE(plannerCli.getNumMigrations() == 1);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI gather", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("gather");

    // Call the functions
    plannerCli.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI hello world", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("hello-world");

    // Call the functions
    plannerCli.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI async. send recv", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("isendrecv");

    // Call the functions
    plannerCli.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI order", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("order");

    // Call the functions
    plannerCli.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI reduce", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("reduce");

    // Call the functions
    plannerCli.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI reduce many times", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("reduce-many");

    // Call the functions
    plannerCli.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI scan", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("scan");

    // Call the functions
    plannerCli.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI scatter", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("scatter");

    // Call the functions
    plannerCli.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI send", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("send");

    // Call the functions
    plannerCli.callFunctions(req);

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
    plannerCli.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test sending many MPI messages", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("send-many");

    // Call the functions
    plannerCli.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI send-recv", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("sendrecv");

    // Call the functions
    plannerCli.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI status", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("status");

    // Call the functions
    plannerCli.callFunctions(req);

    checkAllocationAndResult(req);
}

TEST_CASE_METHOD(MpiDistTestsFixture, "Test MPI types sizes", "[mpi]")
{
    // Set up this host's resources
    setLocalSlots(nLocalSlots);
    auto req = setRequest("typesize");

    // Call the functions
    plannerCli.callFunctions(req);

    checkAllocationAndResult(req);
}
}
