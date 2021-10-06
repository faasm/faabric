#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/transport/PointToPointCall.h>
#include <faabric/transport/PointToPointClient.h>
#include <faabric/transport/common.h>
#include <faabric/transport/macros.h>

namespace faabric::transport {

PointToPointClient::PointToPointClient(const std::string& hostIn)
  : faabric::transport::MessageEndpointClient(hostIn,
                                              POINT_TO_POINT_ASYNC_PORT,
                                              POINT_TO_POINT_SYNC_PORT)
{}

void PointToPointClient::sendMappings(faabric::PointToPointMappings& mappings)
{
    faabric::EmptyResponse resp;
    syncSend(PointToPointCall::MAPPING, &mappings, &resp);
}

void PointToPointClient::sendMessage(faabric::PointToPointMessage& msg)
{
    asyncSend(PointToPointCall::MESSAGE, &msg);
}
}
