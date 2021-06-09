#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/transport/MessageEndpoint.h>
#include <faabric/transport/common.h>
#include <faabric/transport/macros.h>

namespace faabric::transport {
/* These two abstract methods are used to broadcast the host-rank mapping at
 * initialisation time.
 */
faabric::MpiHostsToRanksMessage recvMpiHostRankMsg();

void sendMpiHostRankMsg(const std::string& hostIn,
                        const faabric::MpiHostsToRanksMessage msg);

/* This class abstracts the notion of a communication channel between two MPI
 * ranks. There will always be one rank local to this host, and one remote.
 * Note that the port is unique per (user, function, sendRank, recvRank) tuple.
 */
class MpiMessageEndpoint
{
  public:
    MpiMessageEndpoint(const std::string& hostIn, int portIn);

    void sendMpiMessage(const std::shared_ptr<faabric::MPIMessage>& msg);

    std::shared_ptr<faabric::MPIMessage> recvMpiMessage();

    void close();

  private:
    SendMessageEndpoint sendMessageEndpoint;
    RecvMessageEndpoint recvMessageEndpoint;
};
}
