#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/state/State.h>
#include <faabric/state/StateCommon.h>
#include <faabric/transport/MessageEndpointServer.h>

namespace faabric::state {
class StateServer final : public faabric::transport::MessageEndpointServer
{
  public:
    explicit StateServer(State& stateIn);

  private:
    State& state;

    void doRecv(const void* headerData,
                int headerSize,
                const void* bodyData,
                int bodySize) override;

    void sendEmptyResponse(const std::string& returnHost);

    void sendResponse(char* serialisedMsg,
                      int size,
                      const std::string& returnHost);

    void recvSize(const void* data, int size);

    void recvPull(const void* data, int size);

    void recvPush(const void* data, int size);

    void recvAppend(const void* data, int size);

    void recvPullAppended(const void* data, int size);

    void recvClearAppended(const void* data, int size);

    void recvDelete(const void* data, int size);

    void recvLock(const void* data, int size);

    void recvUnlock(const void* data, int size);
};
}
