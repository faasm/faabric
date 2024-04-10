#include <catch2/catch.hpp>

#include "dist_test_fixtures.h"
#include "faabric_utils.h"
#include "init.h"
#include "mpi/mpi_native.h"

#include <faabric/scheduler/Scheduler.h>

namespace tests {

TEST_CASE_METHOD(MpiDistTestsFixture,
                 "Test concurrent MPI applications",
                 "[mpi]")
{
    // Prepare both requests
    auto req1 = setRequest("alltoall-sleep");
    auto req2 = setRequest("alltoall-sleep");

    int worldSize = 4;

    // Make enough space to run both applications
    updateLocalSlots(worldSize);
    updateRemoteSlots(worldSize);

    std::vector<std::string> hosts1;
    std::vector<std::string> hosts2;

    SECTION("Same main host")
    {
        hosts1 = { getMasterIP(), getMasterIP(), getWorkerIP(), getWorkerIP() };
        hosts2 = hosts1;
    }

    SECTION("Different main host")
    {
        hosts1 = { getMasterIP(), getMasterIP(), getWorkerIP(), getWorkerIP() };
        hosts2 = { getWorkerIP(), getWorkerIP(), getMasterIP(), getMasterIP() };
    }

    auto preloadDec1 = std::make_shared<batch_scheduler::SchedulingDecision>(
      req1->appid(), req1->groupid());
    auto preloadDec2 = std::make_shared<batch_scheduler::SchedulingDecision>(
      req2->appid(), req2->groupid());
    for (int i = 0; i < worldSize; i++) {
        preloadDec1->addMessage(hosts1.at(i), 0, 0, i);
        preloadDec2->addMessage(hosts2.at(i), 0, 0, i);
    }
    plannerCli.preloadSchedulingDecision(preloadDec1);
    plannerCli.preloadSchedulingDecision(preloadDec2);

    plannerCli.callFunctions(req1);
    auto actualHostsBefore1 = waitForMpiMessagesInFlight(req1);
    REQUIRE(actualHostsBefore1 == hosts1);

    plannerCli.callFunctions(req2);
    auto actualHostsBefore2 = waitForMpiMessagesInFlight(req2);
    REQUIRE(actualHostsBefore2 == hosts2);

    checkAllocationAndResult(req1, hosts1);
    checkAllocationAndResult(req2, hosts2);
}

TEST_CASE_METHOD(MpiDistTestsFixture,
                 "Test MPI migration with two MPI worlds",
                 "[mpi]")
{
    int worldSize = 4;

    // Prepare both requests:
    // - The first will do work, sleep for five seconds, and do work again
    // - The second will do work and check for migration opportunities
    auto req1 = setRequest("alltoall-sleep");
    auto req2 = setRequest("migration");
    auto& msg = req2->mutable_messages()->at(0);
    msg.set_inputdata(std::to_string(NUM_MIGRATION_LOOPS));

    // Both requests will initially be split evenly between the two hosts. Then
    // one of them will be migrated to make use of the free resources
    std::vector<std::string> hostsAfterMigration;
    SECTION("Migrate main rank")
    {
        updateLocalSlots(4);
        updateRemoteSlots(6);
        hostsAfterMigration =
          std::vector<std::string>(worldSize, getWorkerIP());
    }

    SECTION("Don't migrate main rank")
    {
        updateLocalSlots(6);
        updateRemoteSlots(4);
        hostsAfterMigration =
          std::vector<std::string>(worldSize, getMasterIP());
    }

    std::vector<std::string> hostsBefore1 = {
        getMasterIP(), getMasterIP(), getWorkerIP(), getWorkerIP()
    };
    std::vector<std::string> hostsBefore2 = {
        getMasterIP(), getMasterIP(), getWorkerIP(), getWorkerIP()
    };

    // Preload decisions to force sub-optimal scheduling
    auto preloadDec1 = std::make_shared<batch_scheduler::SchedulingDecision>(
      req1->appid(), req1->groupid());
    auto preloadDec2 = std::make_shared<batch_scheduler::SchedulingDecision>(
      req2->appid(), req2->groupid());
    for (int i = 0; i < worldSize; i++) {
        preloadDec1->addMessage(hostsBefore1.at(i), 0, 0, i);
        preloadDec2->addMessage(hostsBefore2.at(i), 0, 0, i);
    }
    plannerCli.preloadSchedulingDecision(preloadDec1);
    plannerCli.preloadSchedulingDecision(preloadDec2);

    plannerCli.callFunctions(req1);
    auto actualHostsBefore1 = waitForMpiMessagesInFlight(req1);
    REQUIRE(hostsBefore1 == actualHostsBefore1);

    plannerCli.callFunctions(req2);
    auto actualHostsBefore2 = waitForMpiMessagesInFlight(req2);
    REQUIRE(hostsBefore2 == actualHostsBefore2);

    checkAllocationAndResult(req1, hostsBefore1);
    checkAllocationAndResult(req2, hostsAfterMigration);
}

TEST_CASE_METHOD(MpiDistTestsFixture,
                 "Test migrating two MPI applications in parallel",
                 "[mpi]")
{
    // Set the slots for the first request: 2 locally and 2 remote
    int worldSize = 4;
    updateLocalSlots(5);
    updateRemoteSlots(5);

    // Prepare both requests: both will do work and check for migration
    // opportunities
    auto req1 = setRequest("migration");
    req1->mutable_messages(0)->set_inputdata(
      std::to_string(NUM_MIGRATION_LOOPS));
    auto req2 = setRequest("migration");
    req2->mutable_messages(0)->set_inputdata(
      std::to_string(NUM_MIGRATION_LOOPS));

    std::vector<std::string> hostsBefore1 = {
        getMasterIP(), getMasterIP(), getMasterIP(), getWorkerIP()
    };
    std::vector<std::string> hostsBefore2 = {
        getWorkerIP(), getWorkerIP(), getWorkerIP(), getMasterIP()
    };

    // Preload decisions to force sub-optimal scheduling
    auto preloadDec1 = std::make_shared<batch_scheduler::SchedulingDecision>(
      req1->appid(), req1->groupid());
    auto preloadDec2 = std::make_shared<batch_scheduler::SchedulingDecision>(
      req2->appid(), req2->groupid());
    for (int i = 0; i < worldSize; i++) {
        preloadDec1->addMessage(hostsBefore1.at(i), 0, 0, i);
        preloadDec2->addMessage(hostsBefore2.at(i), 0, 0, i);
    }
    plannerCli.preloadSchedulingDecision(preloadDec1);
    plannerCli.preloadSchedulingDecision(preloadDec2);

    plannerCli.callFunctions(req1);
#ifndef FAABRIC_USE_SPINLOCK
    auto actualHostsBefore1 = waitForMpiMessagesInFlight(req1);
    REQUIRE(hostsBefore1 == actualHostsBefore1);
#endif

    plannerCli.callFunctions(req2);
#ifndef FAABRIC_USE_SPINLOCK
    auto actualHostsBefore2 = waitForMpiMessagesInFlight(req2);
    REQUIRE(hostsBefore2 == actualHostsBefore2);
#endif

    auto hostsAfter1 = std::vector<std::string>(4, getMasterIP());
    auto hostsAfter2 = std::vector<std::string>(4, getWorkerIP());
    checkAllocationAndResult(req1, hostsAfter1);
    checkAllocationAndResult(req2, hostsAfter2);
}
}
