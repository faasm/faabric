#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/transport/MessageEndpointClient.h>

namespace faabric::transport {

std::vector<std::pair<std::string, faabric::PointToPointMappings>>
getSentMappings();

std::vector<std::pair<std::string, faabric::PointToPointMessage>>
getSentPointToPointMessages();

void clearSentMessages();

class PointToPointClient : public faabric::transport::MessageEndpointClient
{
  public:
    PointToPointClient(const std::string& hostIn);

    void sendMappings(faabric::PointToPointMappings& mappings);

    void sendMessage(faabric::PointToPointMessage& msg);
};
}
