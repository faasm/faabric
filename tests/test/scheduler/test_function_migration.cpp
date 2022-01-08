#include <catch2/catch.hpp>

#include <faabric_utils.h>
#include <fixtures.h>

#include <faabric/scheduler/FunctionMigrationThread.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/config.h>
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

    // The sleep function sleeps for a set timeout before returning
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

        // Build expected result
        faabric::PendingMigrations expected;
        expected.set_appid(appId);
        auto* migration = expected.add_migrations();
        migration->set_messageid(req->messages().at(1).id());
        migration->set_srchost(hosts.at(1));
        migration->set_dsthost(hosts.at(0));
        expectedMigrations =
          std::make_shared<faabric::PendingMigrations>(expected);
    }

    auto decision = sch.callFunctions(req);

    // Update host resources so that a migration opportunity appears, but will
    // only be detected if migration check period is set.
    updateLocalResources(2, 1);

    sch.checkForMigrationOpportunities();

    auto actualMigrations = sch.canAppBeMigrated(appId);
    if (expectedMigrations == nullptr) {
        REQUIRE(actualMigrations == expectedMigrations);
    } else {
        REQUIRE(actualMigrations->appid() == expectedMigrations->appid());
        REQUIRE(actualMigrations->migrations_size() ==
                expectedMigrations->migrations_size());
        for (int i = 0; i < actualMigrations->migrations_size(); i++) {
            auto actual = actualMigrations->mutable_migrations()->at(i);
            auto expected = expectedMigrations->mutable_migrations()->at(i);
            REQUIRE(actual.messageid() == expected.messageid());
            REQUIRE(actual.srchost() == expected.srchost());
            REQUIRE(actual.dsthost() == expected.dsthost());
        }
    }

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

        // Build expected result
        faabric::PendingMigrations expected;
        expected.set_appid(appId);
        auto* migration = expected.add_migrations();
        migration->set_messageid(req->messages().at(1).id());
        migration->set_srchost(hosts.at(1));
        migration->set_dsthost(hosts.at(0));
        expectedMigrations =
          std::make_shared<faabric::PendingMigrations>(expected);
    }

    sch.checkForMigrationOpportunities();

    auto actualMigrations = sch.canAppBeMigrated(appId);
    if (expectedMigrations == nullptr) {
        REQUIRE(actualMigrations == expectedMigrations);
    } else {
        REQUIRE(actualMigrations->appid() == expectedMigrations->appid());
        REQUIRE(actualMigrations->migrations_size() ==
                expectedMigrations->migrations_size());
        for (int i = 0; i < actualMigrations->migrations_size(); i++) {
            auto actual = actualMigrations->mutable_migrations()->at(i);
            auto expected = expectedMigrations->mutable_migrations()->at(i);
            REQUIRE(actual.messageid() == expected.messageid());
            REQUIRE(actual.srchost() == expected.srchost());
            REQUIRE(actual.dsthost() == expected.dsthost());
        }
    }

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
        faabric::PendingMigrations expected;
        expected.set_appid(appId);
        // Migration 1: migrate last message (originally scheduled to last host)
        // to first host. This fills up the first host.
        auto* migration1 = expected.add_migrations();
        migration1->set_messageid(req->messages().at(3).id());
        migration1->set_srchost(hosts.at(3));
        migration1->set_dsthost(hosts.at(0));
        // Migration 2: migrate penultimate message (originally scheduled to
        // penultimate host) to second host. This fills up the second host.
        auto* migration2 = expected.add_migrations();
        migration2->set_messageid(req->messages().at(2).id());
        migration2->set_srchost(hosts.at(2));
        migration2->set_dsthost(hosts.at(1));
        expectedMigrations =
          std::make_shared<faabric::PendingMigrations>(expected);
    }

    sch.checkForMigrationOpportunities();

    auto actualMigrations = sch.canAppBeMigrated(appId);
    if (expectedMigrations == nullptr) {
        REQUIRE(actualMigrations == expectedMigrations);
    } else {
        REQUIRE(actualMigrations->appid() == expectedMigrations->appid());
        REQUIRE(actualMigrations->migrations_size() ==
                expectedMigrations->migrations_size());
        for (int i = 0; i < actualMigrations->migrations_size(); i++) {
            auto actual = actualMigrations->mutable_migrations()->at(i);
            auto expected = expectedMigrations->mutable_migrations()->at(i);
            REQUIRE(actual.messageid() == expected.messageid());
            REQUIRE(actual.srchost() == expected.srchost());
            REQUIRE(actual.dsthost() == expected.dsthost());
        }
    }

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

    SECTION("Can not migrate") { expectedMigrations = nullptr; }

    // As we don't update the available resources, no migration opportunities
    // will appear, even though we are checking for them
    SECTION("Can migrate")
    {
        // Update host resources so that a migration opportunity appears
        updateLocalResources(2, 1);

        // Build expected result
        faabric::PendingMigrations expected;
        expected.set_appid(appId);
        auto* migration = expected.add_migrations();
        migration->set_messageid(req->messages().at(1).id());
        migration->set_srchost(hosts.at(1));
        migration->set_dsthost(hosts.at(0));
        expectedMigrations =
          std::make_shared<faabric::PendingMigrations>(expected);
    }

    // Instead of directly calling the scheduler function to check for migration
    // opportunites, sleep for enough time (twice the check period) so that a
    // migration is detected by the background thread.
    SLEEP_MS(2 * checkPeriodSecs * 1000);

    auto actualMigrations = sch.canAppBeMigrated(appId);
    if (expectedMigrations == nullptr) {
        REQUIRE(actualMigrations == expectedMigrations);
    } else {
        REQUIRE(actualMigrations->appid() == expectedMigrations->appid());
        REQUIRE(actualMigrations->migrations_size() ==
                expectedMigrations->migrations_size());
        for (int i = 0; i < actualMigrations->migrations_size(); i++) {
            auto actual = actualMigrations->mutable_migrations()->at(i);
            auto expected = expectedMigrations->mutable_migrations()->at(i);
            REQUIRE(actual.messageid() == expected.messageid());
            REQUIRE(actual.srchost() == expected.srchost());
            REQUIRE(actual.dsthost() == expected.dsthost());
        }
    }

    faabric::Message res =
      sch.getFunctionResult(req->messages().at(0).id(), 2 * timeToSleep);
    REQUIRE(res.returnvalue() == 0);

    // Check that after the result is set, the app can't be migrated no more
    sch.checkForMigrationOpportunities();
    REQUIRE(sch.canAppBeMigrated(appId) == nullptr);
}

}
