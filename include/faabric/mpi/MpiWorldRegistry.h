#pragma once

#include <faabric/mpi/MpiWorld.h>
#include <faabric/util/concurrent_map.h>

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

    void clearWorld(int worldId);

    void clear();

  private:
    faabric::util::ConcurrentMap<int, std::shared_ptr<MpiWorld>> worldMap;
};

MpiWorldRegistry& getMpiWorldRegistry();
}
