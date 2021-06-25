#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/transport/MessageEndpoint.h>
#include <faabric/transport/MessageEndpointClient.h>
#include <faabric/transport/common.h>
#include <faabric/transport/macros.h>

namespace faabric::transport {
/*
 * These two abstract methods are used to broadcast the host-rank mapping at
 * initialisation time.
 */
faabric::MpiHostsToRanksMessage recvMpiHostRankMsg();

void sendMpiHostRankMsg(const std::string& hostIn,
                        const faabric::MpiHostsToRanksMessage msg);

/*
 * This class abstracts the notion of a communication channel between two remote
 * MPI ranks. There will always be one rank local to this host, and one remote.
 * Note that the ports are unique per (user, function, sendRank, recvRank)
 * tuple.
 *
 * This is different to our normal messaging clients as it wraps two _async_
 * sockets.
 */
class MpiMessageEndpoint
{
  public:
    MpiMessageEndpoint(const std::string& hostIn, int sendPort, int recvPort);

    void sendMpiMessage(const std::shared_ptr<faabric::MPIMessage>& msg);

    std::shared_ptr<faabric::MPIMessage> recvMpiMessage();

  private:
    std::string host;

    AsyncSendMessageEndpoint sendSocket;
    AsyncRecvMessageEndpoint recvSocket;
};
}
