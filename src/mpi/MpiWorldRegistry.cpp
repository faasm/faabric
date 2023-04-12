#include <faabric/mpi/MpiWorldRegistry.h>
#include <faabric/util/config.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>

namespace faabric::mpi {
MpiWorldRegistry& getMpiWorldRegistry()
{
    static MpiWorldRegistry r;
    return r;
}

MpiWorld& MpiWorldRegistry::createWorld(faabric::Message& msg,
                                        int worldId,
                                        std::string hostOverride)
{
    if (worldMap.count(worldId) > 0) {
        SPDLOG_ERROR("World {} already exists", worldId);
        throw std::runtime_error("World already exists");
    }

    faabric::util::SystemConfig& conf = faabric::util::getSystemConfig();
    int worldSize = msg.mpiworldsize();
    if (worldSize <= 0) {
        worldSize = conf.defaultMpiWorldSize;
    }

    faabric::util::FullLock lock(registryMutex);
    MpiWorld& world = worldMap[worldId];

    if (!hostOverride.empty()) {
        world.overrideHost(hostOverride);
    }

    world.create(msg, worldId, worldSize);

    return worldMap[worldId];
}

MpiWorld& MpiWorldRegistry::getOrInitialiseWorld(faabric::Message& msg)
{
    // Create world locally if not exists
    int worldId = msg.mpiworldid();
    if (worldMap.find(worldId) == worldMap.end()) {
        faabric::util::FullLock lock(registryMutex);
        if (worldMap.find(worldId) == worldMap.end()) {
            MpiWorld& world = worldMap[worldId];
            world.initialiseFromMsg(msg);
        }
    }

    {
        faabric::util::SharedLock lock(registryMutex);
        MpiWorld& world = worldMap[worldId];
        world.setMsgForRank(msg);
        return world;
    }
}

MpiWorld& MpiWorldRegistry::getWorld(int worldId)
{
    if (worldMap.count(worldId) == 0) {
        SPDLOG_ERROR("World {} not initialised", worldId);
        throw std::runtime_error("World not initialised");
    }

    return worldMap[worldId];
}

void MpiWorldRegistry::clear()
{
    faabric::util::FullLock lock(registryMutex);
    worldMap.clear();
}
}
