#include <faabric/proto/faabric.pb.h>
#include <faabric/transport/PointToPointClient.h>
#include <faabric/transport/common.h>
#include <faabric/transport/macros.h>

namespace faabric::transport {

PointToPointClient::PointToPointClient(const std::string& hostIn)
  : faabric::transport::MessageEndpointClient(hostIn,
                                              POINT_TO_POINT_ASYNC_PORT,
                                              POINT_TO_POINT_SYNC_PORT)
{}

void PointToPointClient::broadcastPointToPointMappings(
  int appId,
  std::map<int, std::string> idxToHosts)
{
    message::PointToPointMappings mappings;
    for (auto m : idxToHosts) {
        auto appendedValue = mappings->add_mappings();
        appendedValue->set_appid(m.first);
        appendedValue->set_recvidx(m.second);
    }

    // TODO get hosts
    // TODO send mappings to all of them
}

void PointToPointClient::send(int appId,
                              int sendIdx,
                              int recvIdx,
                              const uint8_t* data,
                              size_t dataSize)
{
    faabric::PointToPointMessage msg;
    msg.set_appid(appId);
    msg.set_sendidx(sendIdx);
    msg.set_recvidx(recvIdx);
    msg.set_data(data, dataSize);

    asyncSend(1, &msg);
}
}
