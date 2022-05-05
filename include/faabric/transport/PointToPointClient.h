#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/transport/MessageEndpointClient.h>
#include <faabric/transport/PointToPointCall.h>

namespace faabric::transport {

std::vector<std::pair<std::string, faabric::PointToPointMappings>>
getSentMappings();

std::vector<std::pair<std::string, faabric::PointToPointMessage>>
getSentPointToPointMessages();

std::vector<std::tuple<std::string,
                       faabric::transport::PointToPointCall,
                       faabric::PointToPointMessage>>
getSentLockMessages();

void clearSentMessages();

class PointToPointClient : public faabric::transport::MessageEndpointClient
{
  public:
    PointToPointClient(const std::string& hostIn);

    void sendMappings(faabric::PointToPointMappings& mappings);

    void sendMessage(faabric::PointToPointMessage& msg,
                     int sequenceNum = NO_SEQUENCE_NUM);

    void groupLock(int appId,
                   int groupId,
                   int groupIdx,
                   bool recursive = false);

    void groupUnlock(int appId,
                     int groupId,
                     int groupIdx,
                     bool recursive = false);

  private:
    void makeCoordinationRequest(int appId,
                                 int groupId,
                                 int groupIdx,
                                 faabric::transport::PointToPointCall call);
};
}
