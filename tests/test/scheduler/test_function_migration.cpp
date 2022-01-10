#include <catch2/catch.hpp>

#include <faabric_utils.h>
#include <fixtures.h>

#include <faabric/scheduler/FunctionMigrationThread.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/config.h>
#include <faabric/util/message.h>
#include <faabric/util/testing.h>

using namespace faabric::scheduler;

namespace tests {
class FunctionMigrationTestFixture : public SchedulerTestFixture
{
  public:
    FunctionMigrationTestFixture()
    {
        faabric::util::setMockMode(true);

        std::shared_ptr<TestExecutorFactory> fac =
          std::make_shared<TestExecutorFactory>();
        setExecutorFactory(fac);
    }

    ~FunctionMigrationTestFixture()
    {
        faabric::util::setMockMode(false);

        // Remove all hosts from global set
        for (const std::string& host : sch.getAvailableHosts()) {
            sch.removeHostFromGlobalSet(host);
        }
    }

  protected:
    FunctionMigrationThread migrationThread;
    std::string masterHost = faabric::util::getSystemConfig().endpointHost;

    // Helper method to set the available hosts and slots per host prior to
    // making a scheduling decision
    void setHostResources(std::vector<std::string> registeredHosts,
                          std::vector<int> slotsPerHost,
                          std::vector<int> usedSlotsPerHost)
    {
        assert(registeredHosts.size() == slotsPerHost.size());
        auto& sch = faabric::scheduler::getScheduler();
        sch.clearRecordedMessages();

        for (int i = 0; i < registeredHosts.size(); i++) {
            faabric::HostResources resources;
            resources.set_slots(slotsPerHost.at(i));
            resources.set_usedslots(usedSlotsPerHost.at(i));

            sch.addHostToGlobalSet(registeredHosts.at(i));

            // If setting resources for the master host, update the scheduler.
            // Otherwise, queue the resource response
            if (i == 0) {
                sch.setThisHostResources(resources);
            } else {
                faabric::scheduler::queueResourceResponse(registeredHosts.at(i),
                                                          resources);
            }
        }
    }

    void updateLocalResources(int slots, int usedSlots)
    {
        faabric::HostResources r;
        r.set_slots(slots);
        r.set_usedslots(usedSlots);
        sch.setThisHostResources(r);
    }

    std::shared_ptr<faabric::PendingMigrations>
    buildPendingMigrationsExpectation(
      std::shared_ptr<faabric::BatchExecuteRequest> req,
      std::vector<std::string> hosts,
      std::vector<std::pair<int, int>> migrations)
    {
        faabric::PendingMigrations expected;
        expected.set_appid(req->messages().at(0).appid());

        for (auto pair : migrations) {
            auto* migration = expected.add_migrations();
            auto* migrationMsg = migration->mutable_msg();
            faabric::util::copyMessage(&req->mutable_messages()->at(pair.first),
                                       migrationMsg);
            migration->set_srchost(hosts.at(pair.first));
            migration->set_dsthost(hosts.at(pair.second));
        }

        return std::make_shared<faabric::PendingMigrations>(expected);
    }

    void checkPendingMigrationsExpectation(
      std::shared_ptr<faabric::PendingMigrations> expectedMigrations,
      std::shared_ptr<faabric::PendingMigrations> actualMigrations,
      std::vector<std::string> hosts,
      bool skipMsgIdCheck = false)
    {
        if (expectedMigrations == nullptr) {
            REQUIRE(actualMigrations == expectedMigrations);
        } else {
            // Check actual migration matches expectation
            REQUIRE(actualMigrations->appid() == expectedMigrations->appid());
            REQUIRE(actualMigrations->migrations_size() ==
                    expectedMigrations->migrations_size());
            for (int i = 0; i < actualMigrations->migrations_size(); i++) {
                auto actual = actualMigrations->mutable_migrations()->at(i);
                auto expected = expectedMigrations->mutable_migrations()->at(i);
                if (!skipMsgIdCheck) {
                    REQUIRE(actual.msg().id() == expected.msg().id());
                }
                REQUIRE(actual.srchost() == expected.srchost());
                REQUIRE(actual.dsthost() == expected.dsthost());
            }

            // Check we have sent a message to all other hosts with the pending
            // migration
            auto pendingRequests = getAddPendingMigrationRequests();
            REQUIRE(pendingRequests.size() == hosts.size() - 1);
            for (auto& pendingReq : getAddPendingMigrationRequests()) {
                std::string host = pendingReq.first;
                std::shared_ptr<faabric::PendingMigrations> migration =
                  pendingReq.second;
                auto it = std::find(hosts.begin(), hosts.end(), host);
                REQUIRE(it != hosts.end());
                REQUIRE(migration == actualMigrations);
            }
        }
    }
};

TEST_CASE_METHOD(FunctionMigrationTestFixture,
                 "Test starting and stopping the function migration thread",
                 "[scheduler]")
{
    int wakeUpPeriodSeconds = 2;
    migrationThread.start(wakeUpPeriodSeconds);

    SLEEP_MS(SHORT_TEST_TIMEOUT_MS);

    migrationThread.stop();
}

TEST_CASE_METHOD(
  FunctionMigrationTestFixture,
  "Test migration oportunities are only detected if set in the message",
  "[scheduler]")
{
    // First set resources before calling the functions: one will be allocated
    // locally, another one in the remote host
    std::vector<std::string> hosts = { masterHost, "hostA" };
    std::vector<int> slots = { 1, 1 };
    std::vector<int> usedSlots = { 0, 0 };
    setHostResources(hosts, slots, usedSlots);

    // Second, prepare the request we will migrate in-flight.
    // NOTE: the sleep function sleeps for a set timeout before returning.
    auto req = faabric::util::batchExecFactory("foo", "sleep", 2);
    int timeToSleep = SHORT_TEST_TIMEOUT_MS;
    req->mutable_messages()->at(0).set_inputdata(std::to_string(timeToSleep));
    uint32_t appId = req->messages().at(0).appid();

    // Build expected pending migrations
    std::shared_ptr<faabric::PendingMigrations> expectedMigrations;
    SECTION("Migration not enabled") { expectedMigrations = nullptr; }

    SECTION("Migration enabled")
    {
        // Set to a non-zero value so that migration is enabled
        req->mutable_messages()->at(0).set_migrationcheckperiod(2);

        // Build expected migrations
        std::vector<std::pair<int, int>> migrations = { { 1, 0 } };
        expectedMigrations =
          buildPendingMigrationsExpectation(req, hosts, migrations);
    }

    auto decision = sch.callFunctions(req);

    // Update host resources so that a migration opportunity appears, but will
    // only be detected if migration check period is set.
    updateLocalResources(2, 1);

    sch.checkForMigrationOpportunities();

    auto actualMigrations = sch.canAppBeMigrated(appId);
    checkPendingMigrationsExpectation(
      expectedMigrations, actualMigrations, hosts);

    faabric::Message res =
      sch.getFunctionResult(req->messages().at(0).id(), 2 * timeToSleep);
    REQUIRE(res.returnvalue() == 0);

    // Check that after the result is set, the app can't be migrated no more
    sch.checkForMigrationOpportunities();
    REQUIRE(sch.canAppBeMigrated(appId) == nullptr);
}

TEST_CASE_METHOD(FunctionMigrationTestFixture,
                 "Test checking for migration opportunities",
                 "[scheduler]")
{
    std::vector<std::string> hosts = { masterHost, "hostA" };
    std::vector<int> slots = { 1, 1 };
    std::vector<int> usedSlots = { 0, 0 };
    setHostResources(hosts, slots, usedSlots);

    auto req = faabric::util::batchExecFactory("foo", "sleep", 2);
    int timeToSleep = SHORT_TEST_TIMEOUT_MS;
    req->mutable_messages()->at(0).set_inputdata(std::to_string(timeToSleep));
    uint32_t appId = req->messages().at(0).appid();

    // By setting the check period to a non-zero value, we are effectively
    // opting in to be considered for migration
    req->mutable_messages()->at(0).set_migrationcheckperiod(2);

    auto decision = sch.callFunctions(req);

    std::shared_ptr<faabric::PendingMigrations> expectedMigrations;

    // As we don't update the available resources, no migration opportunities
    // will appear, even though we are checking for them
    SECTION("Can not migrate") { expectedMigrations = nullptr; }

    SECTION("Can migrate")
    {
        // Update host resources so that a migration opportunity appears
        updateLocalResources(2, 1);

        // Build expected migrations
        std::vector<std::pair<int, int>> migrations = { { 1, 0 } };
        expectedMigrations =
          buildPendingMigrationsExpectation(req, hosts, migrations);
    }

    sch.checkForMigrationOpportunities();

    auto actualMigrations = sch.canAppBeMigrated(appId);
    checkPendingMigrationsExpectation(
      expectedMigrations, actualMigrations, hosts);

    faabric::Message res =
      sch.getFunctionResult(req->messages().at(0).id(), 2 * timeToSleep);
    REQUIRE(res.returnvalue() == 0);

    // Check that after the result is set, the app can't be migrated no more
    sch.checkForMigrationOpportunities();
    REQUIRE(sch.canAppBeMigrated(appId) == nullptr);
}

TEST_CASE_METHOD(
  FunctionMigrationTestFixture,
  "Test detecting migration opportunities for several messages and hosts",
  "[scheduler]")
{
    // First set resources before calling the functions: one request will be
    // allocated to each host
    std::vector<std::string> hosts = { masterHost, "hostA", "hostB", "hostC" };
    std::vector<int> slots = { 1, 1, 1, 1 };
    std::vector<int> usedSlots = { 0, 0, 0, 0 };
    setHostResources(hosts, slots, usedSlots);

    auto req = faabric::util::batchExecFactory("foo", "sleep", 4);
    int timeToSleep = SHORT_TEST_TIMEOUT_MS;
    req->mutable_messages()->at(0).set_inputdata(std::to_string(timeToSleep));
    uint32_t appId = req->messages().at(0).appid();

    // Opt in to be considered for migration
    req->mutable_messages()->at(0).set_migrationcheckperiod(2);

    auto decision = sch.callFunctions(req);

    // Set up expectations
    std::shared_ptr<faabric::PendingMigrations> expectedMigrations;
    SECTION("Can not migrate") { expectedMigrations = nullptr; }

    SECTION("Can migrate")
    {
        // Update host resources so that two migration opportunities appear in
        // different hosts.
        std::vector<int> newSlots = { 2, 2, 1, 1 };
        std::vector<int> newUsedSlots = { 1, 1, 1, 1 };
        setHostResources(hosts, newSlots, newUsedSlots);

        // Build expected result: two migrations
        std::vector<std::pair<int, int>> migrations = { { 3, 0 }, { 2, 1 } };
        expectedMigrations =
          buildPendingMigrationsExpectation(req, hosts, migrations);
    }

    sch.checkForMigrationOpportunities();

    auto actualMigrations = sch.canAppBeMigrated(appId);
    checkPendingMigrationsExpectation(
      expectedMigrations, actualMigrations, hosts);

    faabric::Message res =
      sch.getFunctionResult(req->messages().at(0).id(), 2 * timeToSleep);
    REQUIRE(res.returnvalue() == 0);

    // Check that after the result is set, the app can't be migrated no more
    sch.checkForMigrationOpportunities();
    REQUIRE(sch.canAppBeMigrated(appId) == nullptr);
}

TEST_CASE_METHOD(
  FunctionMigrationTestFixture,
  "Test function migration thread detects migration opportunities",
  "[scheduler]")
{
    std::vector<std::string> hosts = { masterHost, "hostA" };
    std::vector<int> slots = { 1, 1 };
    std::vector<int> usedSlots = { 0, 0 };
    setHostResources(hosts, slots, usedSlots);

    auto req = faabric::util::batchExecFactory("foo", "sleep", 2);
    int checkPeriodSecs = 1;
    int timeToSleep = 4 * checkPeriodSecs * 1000;
    req->mutable_messages()->at(0).set_inputdata(std::to_string(timeToSleep));
    uint32_t appId = req->messages().at(0).appid();

    // Opt in to be migrated
    req->mutable_messages()->at(0).set_migrationcheckperiod(checkPeriodSecs);

    auto decision = sch.callFunctions(req);

    std::shared_ptr<faabric::PendingMigrations> expectedMigrations;

    // As we don't update the available resources, no migration opportunities
    // will appear, even though we are checking for them
    SECTION("Can not migrate") { expectedMigrations = nullptr; }

    SECTION("Can migrate")
    {
        // Update host resources so that a migration opportunity appears
        updateLocalResources(2, 1);

        // Build expected migrations
        std::vector<std::pair<int, int>> migrations = { { 1, 0 } };
        expectedMigrations =
          buildPendingMigrationsExpectation(req, hosts, migrations);
    }

    // Instead of directly calling the scheduler function to check for migration
    // opportunites, sleep for enough time (twice the check period) so that a
    // migration is detected by the background thread.
    SLEEP_MS(2 * checkPeriodSecs * 1000);

    auto actualMigrations = sch.canAppBeMigrated(appId);
    checkPendingMigrationsExpectation(
      expectedMigrations, actualMigrations, hosts);

    faabric::Message res =
      sch.getFunctionResult(req->messages().at(0).id(), 2 * timeToSleep);
    REQUIRE(res.returnvalue() == 0);

    // Check that after the result is set, the app can't be migrated no more
    sch.checkForMigrationOpportunities();
    REQUIRE(sch.canAppBeMigrated(appId) == nullptr);
}

TEST_CASE_METHOD(FunctionMigrationTestFixture,
                 "Test adding and removing pending migrations manually",
                 "[scheduler]")
{
    auto req = faabric::util::batchExecFactory("foo", "sleep", 2);
    uint32_t appId = req->messages().at(0).appid();
    std::vector<std::string> hosts = { masterHost, "hostA" };
    std::vector<std::pair<int, int>> migrations = { { 1, 0 } };
    auto expectedMigrations =
      buildPendingMigrationsExpectation(req, hosts, migrations);

    // Add migration manually
    REQUIRE(sch.canAppBeMigrated(appId) == nullptr);
    sch.addPendingMigration(expectedMigrations);
    REQUIRE(sch.canAppBeMigrated(appId) == expectedMigrations);

    // Remove migration manually
    sch.removePendingMigration(appId);
    REQUIRE(sch.canAppBeMigrated(appId) == nullptr);
}

TEST_CASE_METHOD(FunctionMigrationTestFixture,
                 "Test MPI function migration points",
                 "[scheduler]")
{
    // Set up host resources
    std::vector<std::string> hosts = { masterHost, "hostA" };
    std::vector<int> slots = { 2, 2 };
    std::vector<int> usedSlots = { 0, 0 };
    setHostResources(hosts, slots, usedSlots);

    // Clear MPI registries
    getMpiWorldRegistry().clear();

    auto req = faabric::util::batchExecFactory("mpi", "sleep", 1);
    int checkPeriodSecs = 1;
    int timeToSleep = 4 * checkPeriodSecs * 1000;

    int worldId = 123;
    int worldSize = 4;
    auto* firstMsg = req->mutable_messages(0);
    firstMsg->set_inputdata(std::to_string(timeToSleep));
    firstMsg->set_ismpi(true);
    firstMsg->set_mpiworldsize(worldSize);
    firstMsg->set_mpiworldid(worldId);
    firstMsg->set_migrationcheckperiod(checkPeriodSecs);
    uint32_t appId = req->messages().at(0).appid();

    // Call function that wil just sleep
    auto decision = sch.callFunctions(req);

    // Manually create the world, and trigger a second function invocation in
    // the remote host
    MpiWorld world;
    world.create(*firstMsg, worldId, worldSize);

    // Update host resources so that a migration opportunity appears
    updateLocalResources(4, 2);

    // Build expected migrations
    std::shared_ptr<faabric::PendingMigrations> expectedMigrations;
    // We need to add to the original request the ones that will be
    // chained by MPI (this is only needed to build the expectation).
    // NOTE: we do it in a copy of the original request, as otherwise TSAN
    // complains about a data race.
    auto reqCopy = faabric::util::batchExecFactory("mpi", "sleep", worldSize);
    for (int i = 0; i < worldSize; i++) {
        reqCopy->mutable_messages(i)->set_appid(firstMsg->appid());
    }
    std::vector<std::pair<int, int>> migrations = { { 1, 0 }, { 1, 0 } };
    expectedMigrations =
      buildPendingMigrationsExpectation(reqCopy, hosts, migrations);

    // Instead of directly calling the scheduler function to check for migration
    // opportunites, sleep for enough time (twice the check period) so that a
    // migration is detected by the background thread.
    SLEEP_MS(2 * checkPeriodSecs * 1000);

    // When checking that a migration has taken place in MPI, we skip the msg
    // id check. Part of the request is build by the runtime, and therefore
    // we don't have access to the actual messages scheduled.
    auto actualMigrations = sch.canAppBeMigrated(appId);
    checkPendingMigrationsExpectation(
      expectedMigrations, actualMigrations, hosts, true);

    // Check that certain MPI calls actually do the migration
    SECTION("MPI barrier triggers a migration point") { world.barrier(0); }

    SECTION("MPI all reduce triggers a migration point")
    {
        std::vector<int> messageData = { 0, 1, 2 };
        world.allReduce(0,
                        BYTES(messageData.data()),
                        BYTES(messageData.data()),
                        MPI_INT,
                        messageData.size(),
                        MPI_SUM);
    }

    // When performing the migration, MPI will remove it from the pending
    // migrations map
    REQUIRE(sch.canAppBeMigrated(appId) == nullptr);
    checkPendingMigrationsExpectation(
      expectedMigrations, getMpiMockedPendingMigrations().front(), hosts, true);

    faabric::Message res =
      sch.getFunctionResult(firstMsg->id(), 2 * timeToSleep);
    REQUIRE(res.returnvalue() == 0);

    // Clean up
    world.destroy();
}

}
