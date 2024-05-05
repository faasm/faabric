#include <catch2/catch.hpp>

#include "dist_test_fixtures.h"
#include "faabric_utils.h"
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
                 "Test MPI migration with two MPI worlds (bin-pack)",
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
                 "Test MPI migration with two MPI worlds (compact)",
                 "[mpi]")
{
    updatePlannerPolicy("compact");

    int worldSize = 4;

    // Prepare both requests:
    // - The first will do work, sleep for five seconds, and do work again
    // - The second will do work and check for migration opportunities
    auto req1 = setRequest("alltoall-sleep");
    auto req2 = setRequest("migration");
    auto& msg = req2->mutable_messages()->at(0);
    msg.set_inputdata(std::to_string(NUM_MIGRATION_LOOPS));

    updateLocalSlots(8);
    updateRemoteSlots(8);

    std::vector<std::string> hostsBefore1 = {
        getMasterIP(), getMasterIP(), getMasterIP(), getMasterIP()
    };
    std::vector<std::string> hostsBefore2;

    SECTION("Migrate main rank")
    {
        hostsBefore2 = {
            getWorkerIP(), getWorkerIP(), getWorkerIP(), getWorkerIP()
        };
    }

    SECTION("Don't migrate main rank")
    {
        hostsBefore2 = {
            getMasterIP(), getMasterIP(), getWorkerIP(), getWorkerIP()
        };
    }

    std::vector<std::string> hostsAfterMigration =
      std::vector<std::string>(worldSize, getMasterIP());

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

    checkAllocationAndResult(req1, hostsBefore1);
    checkAllocationAndResult(req2, hostsAfterMigration);

    updatePlannerPolicy("bin-pack");
}

TEST_CASE_METHOD(MpiDistTestsFixture,
                 "Test migrating two MPI applications in parallel (bin-pack)",
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

TEST_CASE_METHOD(
  MpiDistTestsFixture,
  "Test migrating an MPI app as a consequence of an eviction (SPOT)",
  "[mpi]")
{
    updatePlannerPolicy("spot");

    int worldSize = 4;

    // Prepare both requests:
    // - The first will do work, sleep for five seconds, and do work again
    // - The second will do work and check for migration opportunities
    auto req1 = setRequest("alltoall-sleep");
    auto req2 = setRequest("migration");
    auto& msg = req2->mutable_messages()->at(0);
    msg.set_inputdata(std::to_string(NUM_MIGRATION_LOOPS));

    updateLocalSlots(8);
    updateRemoteSlots(8);

    std::vector<std::string> hostsBefore1 = {
        getMasterIP(), getMasterIP(), getMasterIP(), getMasterIP()
    };
    std::vector<std::string> hostsBefore2;
    std::vector<std::string> hostsAfterMigration;

    std::string evictedVmIp;

    SECTION("Migrate main rank")
    {
        hostsBefore2 = {
            getWorkerIP(), getWorkerIP(), getMasterIP(), getMasterIP()
        };
        evictedVmIp = getWorkerIP();
        hostsAfterMigration =
          std::vector<std::string>(worldSize, getMasterIP());
    }

    SECTION("Don't migrate main rank")
    {
        hostsBefore2 = {
            getMasterIP(), getMasterIP(), getWorkerIP(), getWorkerIP()
        };
        evictedVmIp = getWorkerIP();
        hostsAfterMigration =
          std::vector<std::string>(worldSize, getMasterIP());
    }

    SECTION("Migrate all ranks")
    {
        hostsBefore2 = {
            getMasterIP(), getMasterIP(), getMasterIP(), getMasterIP()
        };
        evictedVmIp = getMasterIP();
        hostsAfterMigration =
          std::vector<std::string>(worldSize, getWorkerIP());
    }

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

    // Preload should overwrite the evicted IP, so we can set it before we
    // call callFunctions
    setNextEvictedVmIp({ evictedVmIp });

    plannerCli.callFunctions(req1);
    auto actualHostsBefore1 = waitForMpiMessagesInFlight(req1);
    REQUIRE(hostsBefore1 == actualHostsBefore1);

    plannerCli.callFunctions(req2);

    checkAllocationAndResult(req1, hostsBefore1);
    checkAllocationAndResult(req2, hostsAfterMigration);

    updatePlannerPolicy("bin-pack");
}

TEST_CASE_METHOD(MpiDistTestsFixture,
                 "Test stopping and resuming an MPI application (SPOT)",
                 "[mpi]")
{
    updatePlannerPolicy("spot");

    int worldSize = 4;

    // Prepare both requests:
    // - The first will do work, sleep for five seconds, and do work again
    // - The second will do work and check for migration opportunities
    auto req1 = setRequest("alltoall-sleep");
    auto req2 = setRequest("migration");
    auto& msg = req2->mutable_messages()->at(0);
    msg.set_inputdata(std::to_string(NUM_MIGRATION_LOOPS));

    // Make it so that there is not enough slots to migrate. We will have to
    // wait for the first request to finish to be able to resume the app
    updateLocalSlots(4);
    updateRemoteSlots(4);

    auto hostsBefore1 = std::vector<std::string>(worldSize, getMasterIP());
    // This app will realise it is running on a VM that will be evicted, so
    // it will FREEZE
    auto hostsBefore2 = std::vector<std::string>(worldSize, getWorkerIP());
    std::string evictedVmIp = getWorkerIP();

    // Preload decisions to force the allocation we want
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

    // Mark the worker VM as evicted (note that preload takes preference over
    // eviction marks)
    setNextEvictedVmIp({ evictedVmIp });

    plannerCli.callFunctions(req1);
    auto actualHostsBefore1 = waitForMpiMessagesInFlight(req1);
    REQUIRE(hostsBefore1 == actualHostsBefore1);

    plannerCli.callFunctions(req2);

    // First, if we try to get the batch results it shoud say that the app
    // is not finished (even though it will try to re-schedule it again). To
    // the eyes of the client, a FROZEN app is still running
    auto batchResults2 = plannerCli.getBatchResults(req2);
    REQUIRE(!batchResults2->finished());

    // Second, let's wait for the first request to finish so that more
    // slots free up
    checkAllocationAndResult(req1, hostsBefore1);

    // Third, no apps are currently in-flight (1 finished, 1 frozen)
    auto inFlightApps = getInFlightApps();
    REQUIRE(inFlightApps.apps_size() == 0);
    REQUIRE(inFlightApps.frozenapps_size() == 1);
    REQUIRE(inFlightApps.frozenapps(0).appid() == req2->appid());

    // Fourth, try to get the batch results for the FROZEN app again to trigger
    // an un-FREEZE
    batchResults2 = plannerCli.getBatchResults(req2);
    REQUIRE(!batchResults2->finished());

    // Finally, we should be able to wait on
    auto hostsAfterMigration =
      std::vector<std::string>(worldSize, getMasterIP());
    checkAllocationAndResult(req2, hostsAfterMigration);

    updatePlannerPolicy("bin-pack");
}
}
