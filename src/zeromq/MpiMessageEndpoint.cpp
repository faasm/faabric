#include <faabric/scheduler/MpiWorld.h>
#include <faabric/scheduler/MpiWorldRegistry.h>
#include <faabric/zeromq/MpiMessageEndpoint.h>

namespace faabric::zeromq {
MpiMessageEndpoint::MpiMessageEndpoint()
  : MessageEndpoint(DEFAULT_RPC_HOST, MPI_MESSAGE_PORT)
{}

MpiMessageEndpoint::MpiMessageEndpoint(const std::string& overrideHost)
  : MessageEndpoint(overrideHost, MPI_MESSAGE_PORT)
{}

void MpiMessageEndpoint::sendMpiMessage(std::shared_ptr<faabric::MPIMessage> msg)
{
    if (sockType != faabric::zeromq::ZeroMQSocketType::PUSH) {
        throw std::runtime_error("Can't send from a non-PUSH socket");
    }

    std::string serialisedMsg;
    if (!msg->SerializeToString(&serialisedMsg)) {
        throw std::runtime_error("Error serialising message");
    }

    // Send message
    // TODO - possible to avoid the copy here?
    zmq::message_t zmqMsg(serialisedMsg.data(), serialisedMsg.size());
    sendMessage(zmqMsg);
}

void MpiMessageEndpoint::doHandleMessage(zmq::message_t& msg)
{
    // TODO - possible to reduce copies here
    std::string deserialisedMsg((char *) msg.data(), msg.size());
    faabric::MPIMessage mpiMsg;

    // Deserialise message string
    if (!mpiMsg.ParseFromString(deserialisedMsg)) {
        throw std::runtime_error("Error deserialising message");
    }

    // Enqueue MPI message
    faabric::scheduler::MpiWorldRegistry& registry = faabric::scheduler::getMpiWorldRegistry();
    faabric::scheduler::MpiWorld& world = registry.getWorld(mpiMsg.worldid());
    world.enqueueMessage(mpiMsg);
}
}
