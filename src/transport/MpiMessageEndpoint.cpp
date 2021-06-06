#include <faabric/transport/MpiMessageEndpoint.h>

namespace faabric::transport {
faabric::MpiHostRankMsg recvMpiHostRankMsg()
{
    faabric::transport::RecvMessageEndpoint endpoint(MPI_PORT);
    endpoint.open(faabric::transport::getGlobalMessageContext());
    // TODO - preempt data size somehow
    faabric::transport::Message m = endpoint.recv();
    PARSE_MSG(faabric::MpiHostRankMsg, m.data(), m.size());
    // Note - This may be very slow as we poll until unbound
    endpoint.close();

    return msg;
}

void sendMpiHostRankMsg(const std::string& hostIn,
                        const faabric::MpiHostRankMsg msg)
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
}
