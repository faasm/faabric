#pragma once

#include <faabric/mpi/MpiWorld.h>

namespace faabric::mpi {
class MpiWorldRegistry
{
  public:
    MpiWorldRegistry() = default;

    MpiWorld& createWorld(faabric::Message& msg,
                          int worldId,
                          std::string hostOverride = "");

    MpiWorld& getOrInitialiseWorld(faabric::Message& msg);

    MpiWorld& getWorld(int worldId);

    bool worldExists(int worldId);

    void clear();

  private:
    std::shared_mutex registryMutex;
    std::unordered_map<int, MpiWorld> worldMap;
};

MpiWorldRegistry& getMpiWorldRegistry();
}
