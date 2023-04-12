#include <faabric/mpi/MpiContext.h>
#include <faabric/mpi/MpiWorldRegistry.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/util/gids.h>
#include <faabric/util/logging.h>

namespace faabric::mpi {
MpiContext::MpiContext()
  : isMpi(false)
  , rank(-1)
  , worldId(-1)
{}

int MpiContext::createWorld(faabric::Message& msg)
{

    if (msg.mpirank() > 0) {
        SPDLOG_ERROR("Attempting to initialise world for non-zero rank {}",
                     msg.mpirank());
        throw std::runtime_error("Initialising world on non-zero rank");
    }

    worldId = (int)faabric::util::generateGid();
    SPDLOG_DEBUG("Initialising world {}", worldId);

    // Create the MPI world
    MpiWorldRegistry& reg = getMpiWorldRegistry();
    reg.createWorld(msg, worldId);

    // Set up this context
    isMpi = true;
    rank = 0;

    // Return the world id to store it in the original message
    return worldId;
}

void MpiContext::joinWorld(faabric::Message& msg)
{
    if (!msg.ismpi()) {
        // Not an MPI call
        return;
    }

    isMpi = true;
    worldId = msg.mpiworldid();
    rank = msg.mpirank();

    // Register with the world
    MpiWorldRegistry& registry = getMpiWorldRegistry();
    registry.getOrInitialiseWorld(msg);
}

bool MpiContext::getIsMpi() const
{
    return isMpi;
}

int MpiContext::getRank() const
{
    return rank;
}

int MpiContext::getWorldId() const
{
    return worldId;
}
}
