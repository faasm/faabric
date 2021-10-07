#pragma once

#include <faabric/transport/MessageEndpointServer.h>
#include <faabric/transport/PointToPointRegistry.h>

namespace faabric::transport {

class PointToPointBroker final : public MessageEndpointServer
{
  public:
    PointToPointBroker();

  private:
    PointToPointRegistry &reg;

    void doAsyncRecv(int header,
                     const uint8_t* buffer,
                     size_t bufferSize) override;

    std::unique_ptr<google::protobuf::Message>
    doSyncRecv(int header, const uint8_t* buffer, size_t bufferSize) override;
};
}
