#include <catch2/catch.hpp>

#include "faabric_utils.h"
#include "fixtures.h"

#include <faabric/mpi/MpiContext.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/random.h>

using namespace faabric::mpi;
using namespace faabric::scheduler;

namespace tests {

TEST_CASE_METHOD(MpiBaseTestFixture, "Check world creation", "[mpi]")
{
    // First call the message so that it is recorded in the planner, and then
    // create the world
    MpiContext c;
    plannerCli.callFunctions(req);
    c.createWorld(msg);

    // Check a new world ID is created
    int worldId = c.getWorldId();
    REQUIRE(worldId > 0);
    msg.set_mpiworldid(worldId);

    // Check this context is set up
    REQUIRE(c.getIsMpi());
    REQUIRE(c.getRank() == 0);

    // Get the world and check it is set up
    MpiWorldRegistry& reg = getMpiWorldRegistry();
    MpiWorld& world = reg.getOrInitialiseWorld(msg);
    REQUIRE(world.getId() == worldId);
    REQUIRE(world.getSize() == worldSize);
    REQUIRE(world.getUser() == user);
    REQUIRE(world.getFunction() == func);

    world.destroy();
}

TEST_CASE_METHOD(MpiBaseTestFixture,
                 "Check world cannot be created for non-zero rank",
                 "[mpi]")
{
    msg.set_mpirank(2);

    // Try creating world
    MpiContext c;
    REQUIRE_THROWS(c.createWorld(msg));
}

TEST_CASE_METHOD(MpiBaseTestFixture, "Check default world size is set", "[mpi]")
{
    // Set a new world size
    auto& conf = faabric::util::getSystemConfig();
    int origSize = conf.defaultMpiWorldSize;
    int defaultWorldSize = 3;
    conf.defaultMpiWorldSize = defaultWorldSize;

    faabric::HostResources res;
    res.set_usedslots(1);
    res.set_slots(defaultWorldSize * 2);
    sch.setThisHostResources(res);

    // Request different sizes
    int requestedWorldSize;
    SECTION("Under zero") { requestedWorldSize = -1; }
    SECTION("Zero") { requestedWorldSize = 0; }

    // Create the world
    MpiContext c;
    plannerCli.callFunctions(req);
    msg.set_mpirank(0);
    msg.set_mpiworldsize(requestedWorldSize);
    c.createWorld(msg);
    int worldId = c.getWorldId();
    msg.set_mpiworldid(worldId);

    // Check that the size is set to the default
    MpiWorldRegistry& reg = getMpiWorldRegistry();
    MpiWorld& world = reg.getOrInitialiseWorld(msg);
    REQUIRE(world.getSize() == defaultWorldSize);

    // Reset config
    conf.defaultMpiWorldSize = origSize;

    world.destroy();

    waitForMpiMessages(req, defaultWorldSize);
}

TEST_CASE_METHOD(MpiBaseTestFixture, "Check joining world", "[mpi]")
{
    const std::string expectedHost =
      faabric::util::getSystemConfig().endpointHost;

    auto reqA = faabric::util::batchExecFactory("mpi", "hellompi", 1);
    auto& msgA = *reqA->mutable_messages(0);
    int worldSize = 6;
    msgA.set_mpiworldsize(worldSize);
    msgA.set_recordexecgraph(true);
    msgA.set_executedhost(expectedHost);

    // Call the request before creating the MPI world
    plannerCli.callFunctions(reqA);

    // Use one context to create the world
    MpiContext cA;
    cA.createWorld(msgA);
    int worldId = cA.getWorldId();

    waitForMpiMessages(reqA, worldSize);
    // Set the function result to have access to the chained messages
    plannerCli.setMessageResult(std::make_shared<Message>(msgA));

    auto chainedMsgs = faabric::util::getChainedFunctions(msgA);
    REQUIRE(chainedMsgs.size() == worldSize - 1);
    auto msgB =
      plannerCli.getMessageResult(msgA.appid(), *chainedMsgs.begin(), 500);

    // Create another context and make sure it's not initialised
    MpiContext cB;
    REQUIRE(!cB.getIsMpi());
    REQUIRE(cB.getWorldId() == -1);
    REQUIRE(cB.getRank() == -1);

    // Join the world
    cB.joinWorld(msgB);

    REQUIRE(cB.getIsMpi());
    REQUIRE(cB.getWorldId() == worldId);
    REQUIRE(cB.getRank() == 1);

    // Check rank is registered to this host
    MpiWorldRegistry& reg = getMpiWorldRegistry();
    MpiWorld& world = reg.getOrInitialiseWorld(msgB);
    const std::string actualHost = world.getHostForRank(1);
    REQUIRE(actualHost == expectedHost);

    world.destroy();
}
}
