#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/transport/MessageEndpointClient.h>
#include <faabric/transport/PointToPointCall.h>

namespace faabric::transport {

std::vector<std::pair<std::string, faabric::PointToPointMappings>>
getSentMappings();

std::vector<std::pair<std::string, faabric::PointToPointMessage>>
getSentPointToPointMessages();

std::vector<std::pair<std::string, faabric::CoordinationRequest>>
getCoordinationRequests();

void clearSentMessages();

class PointToPointClient : public faabric::transport::MessageEndpointClient
{
  public:
    PointToPointClient(const std::string& hostIn);

    void sendMappings(faabric::PointToPointMappings& mappings);

    void sendMessage(faabric::PointToPointMessage& msg);

    void coordinationLock(int32_t groupId,
                          int32_t groupIdx,
                          bool recursive = false);

    void coordinationUnlock(int32_t groupId,
                            int32_t groupIdx,
                            bool recursive = false);

  private:
    void makeCoordinationRequest(int32_t groupId,
                                 int32_t groupIdx,
                                 faabric::transport::PointToPointCall call);
};
}
