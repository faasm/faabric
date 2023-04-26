#pragma once

#include "faabric/scheduler/MpiWorld.h"

namespace faabric::scheduler {
class MpiWorldRegistry
{
  public:
    MpiWorldRegistry() = default;

    scheduler::MpiWorld& createWorld(faabric::Message& msg,
                                     int worldId,
                                     std::string hostOverride = "");

    scheduler::MpiWorld& getOrInitialiseWorld(faabric::Message& msg);

    scheduler::MpiWorld& getWorld(int worldId);

    bool worldExists(int worldId);

    void clear();

  private:
    std::shared_mutex registryMutex;
    std::unordered_map<int, MpiWorld> worldMap;
};

MpiWorldRegistry& getMpiWorldRegistry();
}
