#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/transport/PointToPointCall.h>
#include <faabric/transport/PointToPointClient.h>
#include <faabric/transport/common.h>
#include <faabric/transport/macros.h>
#include <faabric/util/testing.h>

namespace faabric::transport {

static std::vector<std::pair<std::string, faabric::PointToPointMappings>>
  sentMappings;

static std::vector<std::pair<std::string, faabric::PointToPointMessage>>
  sentMessages;

std::vector<std::pair<std::string, faabric::PointToPointMappings>>
getSentMappings()
{
    return sentMappings;
}

std::vector<std::pair<std::string, faabric::PointToPointMessage>>
getSentPointToPointMessages()
{
    return sentMessages;
}

void clearSentMessages()
{
    sentMappings.clear();
    sentMessages.clear();
}

PointToPointClient::PointToPointClient(const std::string& hostIn)
  : faabric::transport::MessageEndpointClient(hostIn,
                                              POINT_TO_POINT_ASYNC_PORT,
                                              POINT_TO_POINT_SYNC_PORT)
{}

void PointToPointClient::sendMappings(faabric::PointToPointMappings& mappings)
{
    if (faabric::util::isMockMode()) {
        sentMappings.emplace_back(host, mappings);
    } else {
        faabric::EmptyResponse resp;
        syncSend(PointToPointCall::MAPPING, &mappings, &resp);
    }
}

void PointToPointClient::sendMessage(faabric::PointToPointMessage& msg)
{
    if (faabric::util::isMockMode()) {
        sentMessages.emplace_back(host, msg);
    } else {
        asyncSend(PointToPointCall::MESSAGE, &msg);
    }
}
}
