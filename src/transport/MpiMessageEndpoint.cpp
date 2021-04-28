#include <faabric/scheduler/MpiWorld.h>
#include <faabric/scheduler/MpiWorldRegistry.h>
#include <faabric/transport/MpiMessageEndpoint.h>

#include <faabric/util/logging.h>

namespace faabric::transport {
MpiMessageEndpoint::MpiMessageEndpoint()
  : MessageEndpoint(DEFAULT_RPC_HOST, MPI_MESSAGE_PORT)
{}

MpiMessageEndpoint::MpiMessageEndpoint(const std::string& overrideHost)
  : MessageEndpoint(overrideHost, MPI_MESSAGE_PORT)
{}

void MpiMessageEndpoint::sendMpiMessage(std::shared_ptr<faabric::MPIMessage> msg)
{
    // Deliberately using heap allocation, so that ZeroMQ can use zero-copy
    size_t msgSize = msg->ByteSizeLong();
    char* serialisedMsg = new char[msgSize];

    if (!msg->SerializeToArray(serialisedMsg, msgSize)) {
        throw std::runtime_error("Error serialising message");
    }
    sendMessage(serialisedMsg, msgSize);
}

void MpiMessageEndpoint::doHandleMessage(const void *msgData, int size)
{
    faabric::MPIMessage mpiMsg;

    // Deserialise message string
    if (!mpiMsg.ParseFromArray(msgData, size)) {
        throw std::runtime_error("Error deserialising message");
    }

    // Enqueue MPI message
    faabric::scheduler::MpiWorldRegistry& registry = faabric::scheduler::getMpiWorldRegistry();
    faabric::scheduler::MpiWorld& world = registry.getWorld(mpiMsg.worldid());
    world.enqueueMessage(mpiMsg);
}
}
