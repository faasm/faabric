#include <catch2/catch.hpp>

#include "faabric_utils.h"
#include "fixtures.h"

#include <faabric/scheduler/MpiContext.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/random.h>

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

    int worldSize = 6;
    msg.set_mpiworldsize(worldSize);

    // Use one context to create the world
    MpiContext cA;
    cA.createWorld(msg);
    int worldId = cA.getWorldId();

    // Get one message formed by world creation. Message 0 corresponds to the
    // world-creating message, messages 1-`worldSize` correspond to the rest
    faabric::Message msgB = sch.getRecordedMessagesAll().at(1);

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
