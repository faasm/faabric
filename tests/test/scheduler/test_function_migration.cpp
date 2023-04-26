#include <catch2/catch.hpp>

#include <faabric_utils.h>
#include <fixtures.h>

#include <faabric/mpi/MpiWorld.h>
#include <faabric/mpi/MpiWorldRegistry.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/config.h>
#include <faabric/util/testing.h>

using namespace faabric::scheduler;

namespace tests {
class FunctionMigrationTestFixture
  : public SchedulerTestFixture
  , public PointToPointClientServerFixture
  , public FunctionCallServerTestFixture
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
            auto resources = std::make_shared<faabric::HostResources>();
            resources->set_slots(slotsPerHost.at(i));
            resources->set_usedslots(usedSlotsPerHost.at(i));
            sch.addHostToGlobalSet(registeredHosts.at(i), resources);
        }
    }

    void updateLocalResources(int slots, int usedSlots)
    {
        auto r = std::make_shared<faabric::HostResources>();
        r->set_slots(slots);
        r->set_usedslots(usedSlots);
        sch.addHostToGlobalSet(masterHost, r);
    }

    void updateGroupId(std::shared_ptr<faabric::BatchExecuteRequest> req,
                       const int newGroupId)
    {
        req->set_groupid(newGroupId);
        for (int i = 0; i < req->messages_size(); i++) {
            req->mutable_messages(i)->set_groupid(newGroupId);
        }
    }

    /*
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
            *migrationMsg = req->mutable_messages()->at(pair.first);
            migration->set_srchost(hosts.at(pair.first));
            migration->set_dsthost(hosts.at(pair.second));
        }

        return std::make_shared<faabric::PendingMigrations>(expected);
    }
    */

    /*
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
            auto pendingRequests = getPendingMigrationsRequests();
            REQUIRE(pendingRequests.size() == hosts.size() - 1);
            for (auto& pendingReq : getPendingMigrationsRequests()) {
                std::string host = pendingReq.first;
                std::shared_ptr<faabric::PendingMigrations> migration =
                  pendingReq.second;
                auto it = std::find(hosts.begin(), hosts.end(), host);
                REQUIRE(it != hosts.end());
                REQUIRE(migration == actualMigrations);
            }
        }
    }
    */
};

TEST_CASE_METHOD(FunctionMigrationTestFixture,
                 "Test we can detect migration opportunities",
                 "[scheduler]")
{
    // First set resources before calling the functions: one will be allocated
    // locally, another one in the remote host
    // TODO: proper mocking of a second host
    std::vector<std::string> hosts = { masterHost, LOCALHOST };
    std::vector<int> slots = { 1, 1 };
    std::vector<int> usedSlots = { 0, 0 };
    setHostResources(hosts, slots, usedSlots);

    // Second, prepare the request we will migrate in-flight.
    auto req = faabric::util::batchExecFactory("foo", "sleep", 2);
    for (int i = 0; i < req->messages_size(); i++) {
        req->mutable_messages(i)->set_groupidx(i);
    }

    auto decision = sch.callFunctions(req);
    updateGroupId(req, decision.groupId);

    // Update host resources so that a migration opportunity appears, but will
    // only be detected if migration check period is set.
    bool mustMigrate;
    SECTION("Must migrate")
    {
        mustMigrate = true;
        updateLocalResources(2, 1);
    }

    SECTION("Must not migrate") { mustMigrate = false; }

    // The group leader (message with index 0) will detect the migration, but
    // does not have to migrate
    auto migration0 =
      sch.checkForMigrationOpportunities(*req->mutable_messages(0));
    if (mustMigrate) {
        REQUIRE(migration0 != nullptr);
        // App id is the same, but group id has changed as the distribution has
        // changed
        REQUIRE(migration0->appid() == decision.appId);
        REQUIRE(migration0->groupid() != decision.groupId);
        // Group idx 0 does not have to migrate
        REQUIRE(migration0->groupidx() == 0);
        REQUIRE(migration0->srchost() == migration0->dsthost());
        REQUIRE(decision.hosts.at(0) == migration0->dsthost());

        // Group idx 1 must migrate. Note that we manually set the new group
        // id, we only have to do this in the tests
        auto migration1 = sch.checkForMigrationOpportunities(
          *req->mutable_messages(1), migration0->groupid());
        REQUIRE(migration1->appid() == decision.appId);
        REQUIRE(migration1->groupid() != decision.groupId);
        // Group idx 0 does not have to migrate
        REQUIRE(migration1->groupidx() == 1);
        REQUIRE(migration1->dsthost() != decision.hosts.at(1));
        REQUIRE(migration1->dsthost() == masterHost);
    } else {
        REQUIRE(migration0 == nullptr);
        auto migration1 = sch.checkForMigrationOpportunities(
          *req->mutable_messages(1), decision.groupId);
        REQUIRE(migration1 == nullptr);
    }

    sch.setFunctionResult(*req->mutable_messages(0));
    sch.setFunctionResult(*req->mutable_messages(1));
}

TEST_CASE_METHOD(FunctionMigrationTestFixture,
                 "Test MPI migration opportunities",
                 "[scheduler]")
{
    // Set up host resources
    std::vector<std::string> hosts = { masterHost, LOCALHOST };
    std::vector<int> slots = { 2, 2 };
    std::vector<int> usedSlots = { 0, 0 };
    setHostResources(hosts, slots, usedSlots);

    // Clear MPI registries
    faabric::mpi::getMpiWorldRegistry().clear();

    // Make sure that the check period is much less than the sleep period
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

    // Call function that wil just sleep
    auto decision = sch.callFunctions(req);
    updateGroupId(req, decision.groupId);

    // Manually create the world, and trigger a second function invocation in
    // the remote host
    faabric::mpi::MpiWorld world;
    world.create(*firstMsg, worldId, worldSize);

    // Update host resources so that a migration opportunity appears
    bool mustMigrate;
    SECTION("Must migrate")
    {
        mustMigrate = true;
        updateLocalResources(4, 2);
    }

    SECTION("Must not migrate") { mustMigrate = false; }

    auto oldDecision = plannerCli.getSchedulingDecision(req);
    // Check the expected migration for each rank. The expected migration will
    // not be null if we have updated the local resoruces. In that case, only
    // group idxs 2 and 3 will migrate
    std::shared_ptr<faabric::PendingMigration> expectedMigration;
    expectedMigration =
      sch.checkForMigrationOpportunities(*req->mutable_messages(0));
    auto newDecision = plannerCli.getSchedulingDecision(req);

    REQUIRE(oldDecision.appId == newDecision.appId);
    REQUIRE(oldDecision.hosts.size() == newDecision.hosts.size());
    if (mustMigrate) {
        REQUIRE(expectedMigration != nullptr);
        REQUIRE(oldDecision.groupId != newDecision.groupId);
        REQUIRE(expectedMigration->appid() == newDecision.appId);
        REQUIRE(expectedMigration->groupid() == newDecision.groupId);
        REQUIRE(expectedMigration->groupidx() == 0);
        REQUIRE(expectedMigration->srchost() == oldDecision.hosts.at(0));
        REQUIRE(expectedMigration->dsthost() == newDecision.hosts.at(0));
    } else {
        REQUIRE(expectedMigration == nullptr);
        REQUIRE(oldDecision.groupId == newDecision.groupId);
    }

    // Impersonate other group idxs
    for (int i = 1; i < worldSize; i++) {
        faabric::Message msg;
        msg.set_appid(oldDecision.appId);
        msg.set_groupid(oldDecision.groupId);
        msg.set_groupidx(i);
        expectedMigration =
          sch.checkForMigrationOpportunities(msg, newDecision.groupId);
        if (mustMigrate) {
            REQUIRE(expectedMigration != nullptr);
            REQUIRE(expectedMigration->appid() == newDecision.appId);
            REQUIRE(expectedMigration->groupid() == newDecision.groupId);
            REQUIRE(expectedMigration->groupidx() == i);
            // Note that we don't check the source host in the expected
            // migration because we set the field to `thisHost`, which we can't
            // impersonate in the tests
            REQUIRE(expectedMigration->dsthost() == newDecision.hosts.at(i));
            // Check that group idxs 2 and 3 migrate to a different host
            if (i > 1) {
                REQUIRE(oldDecision.hosts.at(i) != newDecision.hosts.at(i));
            } else {
                REQUIRE(oldDecision.hosts.at(i) == newDecision.hosts.at(i));
            }
        } else {
            REQUIRE(expectedMigration == nullptr);
        }
    }

    faabric::Message res =
      sch.getFunctionResult(req->messages().at(0), 2 * timeToSleep);
    REQUIRE(res.returnvalue() == 0);

    // Clean up
    world.destroy();
}
}
