#include <faabric/transport/MpiMessageEndpoint.h>
#include <faabric/util/logging.h>
#include <faabric/util/testing.h>

namespace faabric::transport {

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
    std::optional<Message> mMaybe = recvSocket.recv();
    if (!mMaybe.has_value()) {
        throw MessageTimeoutException("Mpi message timeout");
    }
    Message& m = mMaybe.value();
    PARSE_MSG(faabric::MPIMessage, m.data(), m.size());

    return std::make_shared<faabric::MPIMessage>(msg);
}
}
