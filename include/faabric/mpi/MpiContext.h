#pragma once

#include <faabric/mpi/MpiWorldRegistry.h>
#include <faabric/proto/faabric.pb.h>

namespace faabric::mpi {
class MpiContext
{
  public:
    MpiContext();

    int createWorld(faabric::Message& msg);

    void joinWorld(faabric::Message& msg);

    bool getIsMpi() const;

    int getRank() const;

    int getWorldId() const;

  private:
    bool isMpi = false;
    int rank = -1;
    int worldId = -1;
};
}
