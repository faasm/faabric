#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/MpiWorldRegistry.h>

namespace faabric::scheduler {
class MpiContext
{
  public:
    MpiContext();

    int createWorld(faabric::Message& msg);

    void joinWorld(faabric::Message& msg);

    bool getIsMpi();

    int getWorldId();

    int getRank();

  private:
    bool isMpi = false;
    int rank = -1;
    int worldId = -1;
};
}
