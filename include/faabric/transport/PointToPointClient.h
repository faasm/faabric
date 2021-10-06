#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/transport/MessageEndpointClient.h>

namespace faabric::transport {

class PointToPointClient : public faabric::transport::MessageEndpointClient
{
  public:
    PointToPointClient(const std::string& hostIn);

    void sendMappings(faabric::PointToPointMappings& mappings);

    void sendMessage(faabric::PointToPointMessage& msg);
};
}
