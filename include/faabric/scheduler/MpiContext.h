#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/MpiWorldRegistry.h>

namespace faabric::scheduler {
class MpiContext
{
  public:
    MpiContext();

    void createWorld(const faabric::Message& msg);

    void joinWorld(const faabric::Message& msg);

    bool getIsMpi();

    int getWorldId();

    int getRank();

  private:
    bool isMpi;
    int rank;
    int worldId;
};
}
