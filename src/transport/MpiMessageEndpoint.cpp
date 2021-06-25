#include <faabric/transport/MpiMessageEndpoint.h>
#include <faabric/util/logging.h>
#include <faabric/util/testing.h>

namespace faabric::transport {

static std::vector<faabric::MpiHostsToRanksMessage> rankMessages;

faabric::MpiHostsToRanksMessage recvMpiHostRankMsg()
{
    if (faabric::util::isMockMode()) {
        assert(!rankMessages.empty());
        faabric::MpiHostsToRanksMessage msg = rankMessages.back();
        rankMessages.pop_back();
        return msg;
    }

    SPDLOG_TRACE("Receiving MPI host ranks on {}", MPI_BASE_PORT);
    faabric::transport::AsyncRecvMessageEndpoint endpoint(MPI_BASE_PORT);
    faabric::transport::Message m = endpoint.recv();
    PARSE_MSG(faabric::MpiHostsToRanksMessage, m.data(), m.size());

    return msg;
}

void sendMpiHostRankMsg(const std::string& hostIn,
                        const faabric::MpiHostsToRanksMessage msg)
{
    if (faabric::util::isMockMode()) {
        rankMessages.push_back(msg);
        return;
    }

    SPDLOG_TRACE("Sending MPI host ranks to {}:{}", hostIn, MPI_BASE_PORT);
    faabric::transport::AsyncSendMessageEndpoint endpoint(hostIn,
                                                          MPI_BASE_PORT);
    SERIALISE_MSG(msg)
    endpoint.send(buffer, msgSize, false);
}

MpiMessageEndpoint::MpiMessageEndpoint(const std::string& hostIn,
                                       int sendPort,
                                       int recvPort)
  : host(hostIn)
  , sendSocket(hostIn, sendPort)
  , recvSocket(recvPort)
{}

void MpiMessageEndpoint::sendMpiMessage(
  const std::shared_ptr<faabric::MPIMessage>& msg)
{
    SERIALISE_MSG_PTR(msg)
    sendSocket.send(buffer, msgSize, false);
}

std::shared_ptr<faabric::MPIMessage> MpiMessageEndpoint::recvMpiMessage()
{
    Message m = recvSocket.recv();
    PARSE_MSG(faabric::MPIMessage, m.data(), m.size());

    return std::make_shared<faabric::MPIMessage>(msg);
}
}
