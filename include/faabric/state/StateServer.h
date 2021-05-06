#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/state/State.h>
#include <faabric/transport/MessageEndpointServer.h>

namespace faabric::state {
class StateServer final : public faabric::transport::MessageEndpointServer
{
  public:
    explicit StateServer(State& stateIn);

  private:
    State& state;

    /* Send ACK to the client
     *
     * This method is used by calls that want to block, but have no return
     * value. Together with a blocking receive from the client, receiving this
     * message ACKs the remote call.
     */
    void sendEmptyResponse(const std::string& returnHost);

    void doRecv(const void* headerData,
                int headerSize,
                const void* bodyData,
                int bodySize) override;

    /* State server API */

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
