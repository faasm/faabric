#include <faabric/proto/faabric.pb.h>
#include <faabric/transport/MpiMessageEndpoint.h>
#include <faabric/transport/macros.h>

// TODO
// - persist message endpoints?
namespace faabric::transport {
/*
MpiMessageEndpoint::MpiMessageEndpoint(const std::string& hostIn, int rank)
  // : sendEndpoint(hostIn, MPI_PORT + rank)
  // , recvEndpoint(MPI_PORT + rank)
{}
*/

void SendMpiMessage(const std::string& hostIn, int recvRank,
  const std::shared_ptr<faabric::MPIMessage> msg)
{
    size_t msgSize = msg->ByteSizeLong();
    {
        uint8_t sMsg[msgSize];
        if (!msg->SerializeToArray(sMsg, msgSize)) {
            throw std::runtime_error("Error serialising message");
        }
        SendMessageEndpoint endpoint(hostIn, MPI_MESSAGE_PORT + recvRank);
        endpoint.open(getGlobalMessageContext());
        endpoint.send(sMsg, msgSize, false);
        endpoint.close();
    }
}

std::shared_ptr<faabric::MPIMessage> RecvMpiMessage(int recvRank)
{
    RecvMessageEndpoint endpoint(MPI_MESSAGE_PORT + recvRank);
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
