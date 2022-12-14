#pragma once

#include <faabric/transport/MessageEndpointServer.h>
#include <faabric/transport/PointToPointBroker.h>

namespace faabric::transport {

class PointToPointServer final : public MessageEndpointServer
{
  public:
    PointToPointServer();

  private:
    PointToPointBroker& broker;

    void doAsyncRecv(transport::Message& message) override;

    std::unique_ptr<google::protobuf::Message> doSyncRecv(
      transport::Message& message) override;

    void onWorkerStop() override;

    std::unique_ptr<google::protobuf::Message> doRecvMappings(
      std::span<const uint8_t> buffer);

    void recvGroupLock(std::span<const uint8_t> buffer, bool recursive);

    void recvGroupUnlock(std::span<const uint8_t> buffer, bool recursive);
};
}
