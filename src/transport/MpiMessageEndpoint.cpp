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

MpiMessageEndpoint::MpiMessageEndpoint(const std::string& hostIn, int portIn)
  : sendMessageEndpoint(hostIn, portIn)
  , recvMessageEndpoint(portIn)
{
    sendMessageEndpoint.open(faabric::transport::getGlobalMessageContext());
    recvMessageEndpoint.open(faabric::transport::getGlobalMessageContext());
}

MpiMessageEndpoint::MpiMessageEndpoint(const std::string& hostIn,
                                       int portIn,
                                       const std::string& overrideRecvHost)
  : sendMessageEndpoint(hostIn, portIn)
  , recvMessageEndpoint(portIn, overrideRecvHost)
{
    sendMessageEndpoint.open(faabric::transport::getGlobalMessageContext());
    recvMessageEndpoint.open(faabric::transport::getGlobalMessageContext());
}

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

void MpiMessageEndpoint::close()
{
    if (sendMessageEndpoint.socket != nullptr) {
        sendMessageEndpoint.close();
    }
    if (recvMessageEndpoint.socket != nullptr) {
        recvMessageEndpoint.close();
    }
}
}
