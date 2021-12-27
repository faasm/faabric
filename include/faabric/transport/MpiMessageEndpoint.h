#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/transport/MessageEndpoint.h>
#include <faabric/transport/common.h>
#include <faabric/transport/macros.h>

namespace faabric::transport {

// This class abstracts the notion of a communication channel between two remote
// MPI ranks. There will always be one rank local to this host, and one remote.
// Note that the ports are unique per (user, function, sendRank, recvRank)
// tuple.
//
// This is different to our normal messaging clients as it wraps two
// _async_ sockets.

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

class LocalMpiMessageEndpoint
{
  public:
    LocalMpiMessageEndpoint(const std::string& sendLabel,
                            const std::string& recvLabel);

    void sendMpiMessage(const std::shared_ptr<faabric::MPIMessage>& msg);

    std::shared_ptr<faabric::MPIMessage> recvMpiMessage();

  private:
    AsyncDirectSendEndpoint sendSocket;
    AsyncDirectRecvEndpoint recvSocket;
};
}
