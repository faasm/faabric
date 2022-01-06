#include <catch2/catch.hpp>

#include <faabric_utils.h>
#include <fixtures.h>

#include <faabric/proto/faabric.pb.h>
#include <faabric/runner/FaabricMain.h>
#include <faabric/scheduler/ExecutorFactory.h>
#include <faabric/scheduler/FunctionMigrationServer.h>
#include <faabric/util/logging.h>

using namespace faabric::scheduler;

namespace tests {
class FunctionMigrationTestFixture
  : public SchedulingDecisionTestFixture
  , public ConfTestFixture
{
  protected:
    FunctionMigrationServer server;

  public:
    FunctionMigrationTestFixture()
    {
        conf.funcMigration = "on";
        conf.migrationCheckPeriod = 2;
    }
};

TEST_CASE_METHOD(FunctionMigrationTestFixture,
                 "Test starting and stopping the function migration server",
                 "[scheduler]")
{
    SECTION("Disable function migration")
    {
        // No-ops the start and stop calls
        conf.funcMigration = "off";
    }

    SECTION("Enable function migration") { conf.funcMigration = "on"; }

    server.start();

    SLEEP_MS(conf.migrationCheckPeriod * 1000);

    server.stop();
}

TEST_CASE_METHOD(
  FunctionMigrationTestFixture,
  "Test can't run function migration server with non-positive check period",
  "[scheduler]")
{
    conf.migrationCheckPeriod = 0;

    REQUIRE_THROWS(server.start());
}

TEST_CASE_METHOD(
  FunctionMigrationTestFixture,
  "Test function migration server detects migration opportunities",
  "[scheduler]")
{
    // First set resources before calling the functions: one will be allocated
    // locally, another one in the remote host
    std::vector<std::string> hosts = { masterHost, "hostA" };
    std::vector<int> slots = { 1, 1 };
    setHostResources(hosts, slots);

    auto req = faabric::util::batchExecFactory("foo", "migration", 2);
    auto decision = sch.callFunctions(req);
    uint32_t appId = req->messages().at(0).appid();

    // Set up expectations
    std::shared_ptr<faabric::PendingMigrations> expectedMigrations;
    SECTION("Can not migrate") { expectedMigrations = nullptr; }

    SECTION("Can migrate")
    {
        // Update host resources so that a migration opportunity appears
        faabric::HostResources r;
        r.set_slots(2);
        r.set_usedslots(1);
        sch.setThisHostResources(r);

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

    faabric::Message res = sch.getFunctionResult(req->messages().at(0).id(),
                                                 2 * SHORT_TEST_TIMEOUT_MS);
    REQUIRE(res.returnvalue() == 0);

    // Check that after the result is set, the app can't be migrated no more
    sch.checkForMigrationOpportunities();
    REQUIRE(sch.canAppBeMigrated(appId) == nullptr);
}

TEST_CASE_METHOD(
  FunctionMigrationTestFixture,
  "Test detecting migration opportunities with several hosts and requests",
  "[scheduler]")
{
    // First set resources before calling the functions: one will be allocated
    // locally, another one in the remote host
    std::vector<std::string> hosts = { masterHost, "hostA", "hostB", "hostC" };
    std::vector<int> slots = { 1, 1, 1, 1 };
    setHostResources(hosts, slots);

    auto req = faabric::util::batchExecFactory("foo", "migration", 4);
    auto decision = sch.callFunctions(req);
    uint32_t appId = req->messages().at(0).appid();

    // Set up expectations
    std::shared_ptr<faabric::PendingMigrations> expectedMigrations;
    SECTION("Can not migrate") { expectedMigrations = nullptr; }

    SECTION("Can migrate")
    {
        // Update host resources so that a migration opportunity appears
        faabric::HostResources r;
        r.set_slots(2);
        r.set_usedslots(1);
        // This host
        sch.setThisHostResources(r);
        // Host A
        faabric::scheduler::queueResourceResponse("hostA", r);

        // Build expected result
        faabric::PendingMigrations expected;
        expected.set_appid(appId);
        // Migrate last message (scheduled to last host) to first host. This
        // fills up the first host.
        auto* migration1 = expected.add_migrations();
        migration1->set_messageid(req->messages().at(3).id());
        migration1->set_srchost(hosts.at(3));
        migration1->set_dsthost(hosts.at(0));
        // Migrate penultimate message (scheduled to penultimate host) to first
        // host. This fills up the first host.
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

    faabric::Message res = sch.getFunctionResult(req->messages().at(0).id(),
                                                 2 * SHORT_TEST_TIMEOUT_MS);
    REQUIRE(res.returnvalue() == 0);

    // Check that after the result is set, the app can't be migrated no more
    sch.checkForMigrationOpportunities();
    REQUIRE(sch.canAppBeMigrated(appId) == nullptr);
}
}
