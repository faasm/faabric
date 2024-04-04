#include <faabric/mpi/MpiWorldRegistry.h>
#include <faabric/util/config.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>

namespace faabric::mpi {
MpiWorldRegistry& getMpiWorldRegistry()
{
    static MpiWorldRegistry reg;
    return reg;
}

MpiWorld& MpiWorldRegistry::createWorld(faabric::Message& msg,
                                        int worldId,
                                        std::string hostOverride)
{
    if (worldMap.contains(worldId)) {
        SPDLOG_ERROR("World {} already exists", worldId);
        throw std::runtime_error("World already exists");
    }

    faabric::util::SystemConfig& conf = faabric::util::getSystemConfig();
    int worldSize = msg.mpiworldsize();
    if (worldSize <= 0) {
        worldSize = conf.defaultMpiWorldSize;
    }

    worldMap.tryEmplaceThenMutate(
      worldId, [&](bool inserted, std::shared_ptr<MpiWorld>& worldPtr) {
          // Only the first rank will see inserted as true
          if (inserted) {
              worldPtr = std::make_shared<MpiWorld>();

              if (!hostOverride.empty()) {
                  worldPtr->overrideHost(hostOverride);
              }

              worldPtr->create(msg, worldId, worldSize);
          }
      });

    auto world = worldMap.get(worldId).value_or(nullptr);
    if (world == nullptr) {
        SPDLOG_ERROR("World {} points to null!", worldId);
        throw std::runtime_error("Null-pointing MPI world!");
    }

    world->initialiseRankFromMsg(msg);

    return *world;
}

MpiWorld& MpiWorldRegistry::getOrInitialiseWorld(faabric::Message& msg)
{
    // Create world locally if not exists
    int worldId = msg.mpiworldid();
    worldMap.tryEmplaceThenMutate(
      worldId, [&](bool inserted, std::shared_ptr<MpiWorld>& worldPtr) {
          if (inserted) {
              worldPtr = std::make_shared<MpiWorld>();

              worldPtr->initialiseFromMsg(msg);
          }
      });

    auto world = worldMap.get(worldId).value_or(nullptr);
    if (world == nullptr) {
        SPDLOG_ERROR("World {} points to null!", worldId);
        throw std::runtime_error("Null-pointing MPI world!");
    }

    world->initialiseRankFromMsg(msg);

    return *world;
}

MpiWorld& MpiWorldRegistry::getWorld(int worldId)
{
    auto world = worldMap.get(worldId).value_or(nullptr);

    if (world == nullptr) {
        SPDLOG_ERROR("World {} not initialised", worldId);
        throw std::runtime_error("World not initialised");
    }

    return *world;
}

bool MpiWorldRegistry::worldExists(int worldId)
{
    return worldMap.contains(worldId);
}

void MpiWorldRegistry::clear()
{
    worldMap.clear();
}
}
