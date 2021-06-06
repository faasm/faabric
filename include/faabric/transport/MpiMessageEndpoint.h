#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/transport/common.h>
#include <faabric/transport/macros.h>
#include <faabric/transport/MessageEndpoint.h>

namespace faabric::transport {
faabric::MpiHostRankMsg recvMpiHostRankMsg();

void sendMpiHostRankMsg(const std::string& hostIn,
                        const faabric::MpiHostRankMsg msg);
}
