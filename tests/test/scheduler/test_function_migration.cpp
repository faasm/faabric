#include <catch2/catch.hpp>

#include <faabric_utils.h>
#include <fixtures.h>

#include <faabric/mpi/MpiWorld.h>
#include <faabric/mpi/MpiWorldRegistry.h>
#include <faabric/util/config.h>
#include <faabric/util/testing.h>

using namespace faabric::scheduler;

namespace tests {
class FunctionMigrationTestFixture : public SchedulerFixture
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
    std::string mainHost = faabric::util::getSystemConfig().endpointHost;

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

            // If setting resources for the main host, update the scheduler.
            // Otherwise, queue the resource response
            if (i == 0) {
                sch.setThisHostResources(resources);
            } else {
                sch.addHostToGlobalSet(registeredHosts.at(i), std::make_shared<faabric::HostResources>(resources));
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
                 "Test we can detect migration opportunities",
                 "[scheduler]")
{
    // First set resources before calling the functions: one will be allocated
    // locally, another one in the remote host
    // TODO: proper mocking of a second host
    std::vector<std::string> hosts = { mainHost, LOCALHOST };
    std::vector<int> slots = { 1, 1 };
    std::vector<int> usedSlots = { 0, 0 };
    setHostResources(hosts, slots, usedSlots);

    // Second, prepare the request we will migrate in-flight.
    auto req = faabric::util::batchExecFactory("foo", "sleep", 2);
    for (int i = 0; i < req->messages_size(); i++) {
        req->mutable_messages(i)->set_groupidx(i);
    }

    auto decision = plannerCli.callFunctions(req);

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
        REQUIRE(migration1->dsthost() == mainHost);
    } else {
        REQUIRE(migration0 == nullptr);
        auto migration1 = sch.checkForMigrationOpportunities(
          *req->mutable_messages(1), decision.groupId);
        REQUIRE(migration1 == nullptr);
    }

    sch.setFunctionResult(*req->mutable_messages(0));
    sch.setFunctionResult(*req->mutable_messages(1));
}

/*
TEST_CASE_METHOD(FunctionMigrationTestFixture,
                 "Test MPI migration opportunities",
                 "[scheduler]")
{
    // Set up host resources
    std::vector<std::string> hosts = { mainHost, LOCALHOST };
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
    auto decision = plannerCli.callFunctions(req);
    // updateGroupId(req, decision.groupId);

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
*/
}
