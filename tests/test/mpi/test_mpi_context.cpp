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
    MpiContext c;
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
    msg.set_mpirank(0);

    // Set a new world size
    faabric::util::SystemConfig& conf = faabric::util::getSystemConfig();
    int origSize = conf.defaultMpiWorldSize;
    int defaultWorldSize = 12;
    conf.defaultMpiWorldSize = defaultWorldSize;

    // Request different sizes
    int requestedWorldSize;
    SECTION("Under zero") { requestedWorldSize = -1; }
    SECTION("Zero") { requestedWorldSize = 0; }

    // Create the world
    MpiContext c;
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
}

TEST_CASE_METHOD(MpiBaseTestFixture, "Check joining world", "[mpi]")
{
    const std::string expectedHost =
      faabric::util::getSystemConfig().endpointHost;

    // faabric::Message msgA = faabric::util::messageFactory("mpi", "hellompi");
    auto reqA = faabric::util::batchExecFactory("mpi", "hellompi", 1);
    auto& msgA = *reqA->mutable_messages(0);
    int worldSize = 6;
    msgA.set_mpiworldsize(worldSize);
    msgA.set_recordexecgraph(true);

    // Call the request before creating the MPI world
    plannerCli.callFunctions(reqA);

    // Use one context to create the world
    MpiContext cA;
    cA.createWorld(msgA);
    int worldId = cA.getWorldId();

    // Set the function result to have access to the chained messages
    SLEEP_MS(500);
    Scheduler& sch = getScheduler();
    sch.setFunctionResult(msgA);

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
