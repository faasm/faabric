#pragma once

#include <faabric/transport/MessageEndpointClient.h>

namespace faabric::transport {

class PointToPointClient : public faabric::transport::MessageEndpointClient
{
  public:
    PointToPointClient(const std::string& hostIn);

    void broadcastPointToPointMappings(int appId,
                                       std::map<int, std::string> idxToHosts);

    void send(int appId,
              int sendIdx,
              int recvIdx,
              const uint8_t* data,
              size_t dataSize);
};
}
