#include <faabric/transport/MpiMessageEndpoint.h>

namespace faabric::transport {
faabric::MpiHostsToRanksMessage recvMpiHostRankMsg()
{
    faabric::transport::RecvMessageEndpoint endpoint(MPI_PORT);
    endpoint.open(faabric::transport::getGlobalMessageContext());
    faabric::transport::Message m = endpoint.recv();
    PARSE_MSG(faabric::MpiHostsToRanksMessage, m.data(), m.size());
    endpoint.close();

    return msg;
}

void sendMpiHostRankMsg(const std::string& hostIn,
                        const faabric::MpiHostsToRanksMessage msg)
{
    size_t msgSize = msg.ByteSizeLong();
    {
        uint8_t sMsg[msgSize];
        if (!msg.SerializeToArray(sMsg, msgSize)) {
            throw std::runtime_error("Error serialising message");
        }
        faabric::transport::SendMessageEndpoint endpoint(hostIn, MPI_PORT);
        endpoint.open(faabric::transport::getGlobalMessageContext());
        endpoint.send(sMsg, msgSize, false);
        endpoint.close();
    }
}


// TODO - reuse clients!!
void sendMpiMessage(const std::string& hostIn, int portIn,
  const std::shared_ptr<faabric::MPIMessage> msg)
{
    size_t msgSize = msg->ByteSizeLong();
    {
        uint8_t sMsg[msgSize];
        if (!msg->SerializeToArray(sMsg, msgSize)) {
            throw std::runtime_error("Error serialising message");
        }
        SendMessageEndpoint endpoint(hostIn, portIn);
        endpoint.open(getGlobalMessageContext());
        endpoint.send(sMsg, msgSize, false);
        endpoint.close();
    }
}

std::shared_ptr<faabric::MPIMessage> recvMpiMessage(int portIn)
{
    RecvMessageEndpoint endpoint(portIn);
    endpoint.open(getGlobalMessageContext());
    // TODO - preempt data size somehow
    Message m = endpoint.recv();
    PARSE_MSG(faabric::MPIMessage, m.data(), m.size());
    // Note - This may be very slow as we poll until unbound
    endpoint.close();

    // TODO - send normal message, not shared_ptr
    return std::make_shared<faabric::MPIMessage>(msg);
}
}
