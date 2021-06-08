#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/transport/MessageEndpoint.h>
#include <faabric/transport/common.h>
#include <faabric/transport/macros.h>

namespace faabric::transport {
faabric::MpiHostsToRanksMessage recvMpiHostRankMsg();

void sendMpiHostRankMsg(const std::string& hostIn,
                        const faabric::MpiHostsToRanksMessage msg);

void sendMpiMessage(const std::string& hostIn,
                    int portIn,
                    const std::shared_ptr<faabric::MPIMessage> msg);

std::shared_ptr<faabric::MPIMessage> recvMpiMessage(int portIn);
}
