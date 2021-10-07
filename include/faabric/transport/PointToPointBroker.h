#pragma once

#include <faabric/transport/MessageEndpointServer.h>

namespace faabric::transport {

std::string getPointToPointInprocLabel(int appId, int sendIdx, int recvIdx);

class PointToPointBroker final : public MessageEndpointServer
{
  public:
    PointToPointBroker();

  protected:
    std::vector<uint8_t> recvMessage(int appId, int sendIdx, int recvIdx);

  private:
    void doAsyncRecv(int header,
                     const uint8_t* buffer,
                     size_t bufferSize) override;

    std::unique_ptr<google::protobuf::Message>
    doSyncRecv(int header, const uint8_t* buffer, size_t bufferSize) override;

    std::unique_ptr<AsyncInternalSendMessageEndpoint>
    getSendEndpoint(int appId, int sendIdx, int recvIdx);
};
}
