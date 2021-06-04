#pragma once

#include <faabric/transport/MessageEndpoint.h>
#include <faabric/transport/common.h>

namespace faabric::transport {
void SendMpiMessage(const std::string& hostIn,
                    int rank,
                    const std::shared_ptr<faabric::MPIMessage> msg);

// faabric::MPIMessage RecvMpiMessage(const std::string& hostIn, int rank);
std::shared_ptr<faabric::MPIMessage> RecvMpiMessage(int rank);
}
