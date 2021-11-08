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

    void doAsyncRecv(int header,
                     const uint8_t* buffer,
                     size_t bufferSize) override;

    std::unique_ptr<google::protobuf::Message>
    doSyncRecv(int header, const uint8_t* buffer, size_t bufferSize) override;

    void onWorkerStop() override;

    std::unique_ptr<google::protobuf::Message> doRecvMappings(
      const uint8_t* buffer,
      size_t bufferSize);

    void recvGroupLock(const uint8_t* buffer,
                       size_t bufferSize,
                       bool recursive);

    void recvGroupUnlock(const uint8_t* buffer,
                         size_t bufferSize,
                         bool recursive);
};
}
