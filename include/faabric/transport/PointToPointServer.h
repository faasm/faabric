#pragma once

#include <faabric/transport/MessageEndpointServer.h>

namespace faabric::transport {
class PointToPointServer final : public MessageEndpointServer
{
  public:
    PointToPointServer();

  protected:
    std::vector<uint8_t> recvMessage(int appId, int sendIdx, int recvIdx);

  private:
    void doAsyncRecv(int header,
                     const uint8_t* buffer,
                     size_t bufferSize) override;

    std::unique_ptr<google::protobuf::Message>
    doSyncRecv(int header, const uint8_t* buffer, size_t bufferSize) override;

    std::string getInprocLabel(int appId, int sendIdx, int recvIdx);

    std::unique_ptr<AsyncInternalSendMessageEndpoint>
    getSendEndpoint(int appId, int sendIdx, int recvIdx);

    std::unique_ptr<AsyncInternalRecvMessageEndpoint>
    getRecvEndpoint(int appId, int sendIdx, int recvIdx);
};
}
