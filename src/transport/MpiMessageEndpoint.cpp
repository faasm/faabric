#include <faabric/transport/MpiMessageEndpoint.h>

namespace faabric::transport {
faabric::MpiHostsToRanksMessage recvMpiHostRankMsg()
{
    faabric::transport::AsyncRecvMessageEndpoint endpoint(MPI_PORT);
    faabric::transport::Message m = endpoint.recv();
    PARSE_MSG(faabric::MpiHostsToRanksMessage, m.data(), m.size());

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
        faabric::transport::AsyncSendMessageEndpoint endpoint(hostIn, MPI_PORT);
        endpoint.send(sMsg, msgSize, false);
    }
}

MpiMessageEndpoint::MpiMessageEndpoint(const std::string& hostIn, int portIn)
  : sendMessageEndpoint(hostIn, portIn)
  , recvMessageEndpoint(portIn)
{}

MpiMessageEndpoint::MpiMessageEndpoint(const std::string& hostIn,
                                       int sendPort,
                                       int recvPort)
  : sendMessageEndpoint(hostIn, sendPort)
  , recvMessageEndpoint(recvPort)
{}

void MpiMessageEndpoint::sendMpiMessage(
  const std::shared_ptr<faabric::MPIMessage>& msg)
{
    size_t msgSize = msg->ByteSizeLong();
    {
        uint8_t sMsg[msgSize];
        if (!msg->SerializeToArray(sMsg, msgSize)) {
            throw std::runtime_error("Error serialising message");
        }
        sendMessageEndpoint.send(sMsg, msgSize, false);
    }
}

std::shared_ptr<faabric::MPIMessage> MpiMessageEndpoint::recvMpiMessage()
{
    Message m = recvMessageEndpoint.recv();
    PARSE_MSG(faabric::MPIMessage, m.data(), m.size());

    return std::make_shared<faabric::MPIMessage>(msg);
}

void MpiMessageEndpoint::close() {}
}
