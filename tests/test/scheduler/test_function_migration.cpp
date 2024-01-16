#include <catch2/catch.hpp>

#include <faabric_utils.h>
#include <fixtures.h>

#include <faabric/mpi/MpiWorld.h>
#include <faabric/mpi/MpiWorldRegistry.h>
#include <faabric/util/config.h>
#include <faabric/util/testing.h>

using namespace faabric::scheduler;

namespace tests {
class FunctionMigrationTestFixture
  : public SchedulerFixture
  , public FunctionCallClientServerFixture
{
  public:
    FunctionMigrationTestFixture()
    {
        faabric::util::setMockMode(true);

        std::shared_ptr<TestExecutorFactory> fac =
          std::make_shared<TestExecutorFactory>();
        setExecutorFactory(fac);
    }

    ~FunctionMigrationTestFixture() { faabric::util::setMockMode(false); }

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
                sch.addHostToGlobalSet(
                  registeredHosts.at(i),
                  std::make_shared<faabric::HostResources>(resources));
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

    // Set the executed host for correct accounting in the planner when
    // setting the function result
    req->mutable_messages(0)->set_executedhost(mainHost);
    if (mustMigrate) {
        req->mutable_messages(1)->set_executedhost(mainHost);
    } else {
        req->mutable_messages(1)->set_executedhost(LOCALHOST);
    }

    plannerCli.setMessageResult(
      std::make_shared<Message>(*req->mutable_messages(0)));
    plannerCli.setMessageResult(
      std::make_shared<Message>(*req->mutable_messages(1)));
}

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

    // Manually create the world, and trigger a second function invocation in
    // the remote host
    faabric::mpi::MpiWorld world;
    world.create(*firstMsg, worldId, worldSize);

    // TODO: check the original MPI scheduling

    // Get the group ID _after_ we create the world (it is only assigned then)
    int appId = firstMsg->appid();
    int groupId = firstMsg->groupid();

    // Update host resources so that a migration opportunity appears
    bool mustMigrate;
    SECTION("Must migrate")
    {
        mustMigrate = true;
        updateLocalResources(4, 2);
    }

    SECTION("Must not migrate") { mustMigrate = false; }

    // The group leader (message with index 0) will detect the migration, but
    // does not have to migrate
    auto migration0 =
      sch.checkForMigrationOpportunities(*req->mutable_messages(0));
    int newGroupId;
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
        newGroupId = migration0->groupid();
    } else {
        REQUIRE(migration0 == nullptr);
        newGroupId = groupId;
    }

    // Ideally, we checkMigrationOpportunities with each thread's message.
    // Unfortunately, for MPI, the ranks >= 1 messages' are created
    // automatically, as part of a scale change. Thus, in the tests we fake
    // these messages (we just need to set the app id, group id, and group idx)
    for (int i = 1; i < worldSize; i++) {
        faabric::Message msg;
        msg.set_appid(appId);
        msg.set_groupid(groupId);
        msg.set_groupidx(i);
        auto expectedMigration =
          sch.checkForMigrationOpportunities(msg, newGroupId);
        if (mustMigrate) {
            REQUIRE(expectedMigration != nullptr);
            REQUIRE(expectedMigration->appid() == appId);
            REQUIRE(expectedMigration->groupid() != groupId);
            REQUIRE(expectedMigration->groupidx() == i);
            // All ranks migrate to the mainHost
            REQUIRE(expectedMigration->dsthost() == mainHost);
        } else {
            REQUIRE(expectedMigration == nullptr);
        }
    }

    // Clean up
    world.destroy();
}
}
