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
    sendSocket.send(serialisedBuffer, serialisedSize, false);
}

std::shared_ptr<faabric::MPIMessage> MpiMessageEndpoint::recvMpiMessage()
{
    Message msg = recvSocket.recv();
    if (msg.empty()) {
        throw MessageTimeoutException("Mpi message timeout");
    }
    PARSE_MSG(faabric::MPIMessage, msg.data(), msg.size());

    return std::make_shared<faabric::MPIMessage>(parsedMsg);
}
}
