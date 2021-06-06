#include <faabric/scheduler/MpiContext.h>
#include <faabric/scheduler/MpiWorldRegistry.h>

#include <faabric/proto/faabric.pb.h>
#include <faabric/util/gids.h>
#include <faabric/util/logging.h>

namespace faabric::scheduler {
MpiContext::MpiContext()
  : isMpi(false)
  , rank(-1)
  , worldId(-1)
{}

void MpiContext::createWorld(faabric::Message& msg)
{
    const std::shared_ptr<spdlog::logger>& logger = faabric::util::getLogger();

    if (msg.mpirank() > 0) {
        logger->error("Attempting to initialise world for non-zero rank {}",
                      msg.mpirank());
        throw std::runtime_error("Initialising world on non-zero rank");
    }

    worldId = (int)faabric::util::generateGid();
    logger->debug("Initialising world {}", worldId);

    // Update the original message to contain the world ID
    msg.set_mpiworldid(worldId);

    // Create the MPI world
    scheduler::MpiWorldRegistry& reg = scheduler::getMpiWorldRegistry();
    reg.createWorld(msg, worldId);

    // Set up this context
    isMpi = true;
    rank = 0;
}

void MpiContext::joinWorld(const faabric::Message& msg)
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

bool MpiContext::getIsMpi()
{
    return isMpi;
}

int MpiContext::getRank()
{
    return rank;
}

int MpiContext::getWorldId()
{
    return worldId;
}
}
