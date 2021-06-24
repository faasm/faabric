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

    void doAsyncRecv(faabric::transport::Message& header,
                     faabric::transport::Message& body) override;

    faabric::Message doSyncRecv(faabric::transport::Message& header,
                                faabric::transport::Message& body) override;

    /* State server API */

    void recvSize(faabric::transport::Message& body);

    void recvPull(faabric::transport::Message& body);

    void recvPush(faabric::transport::Message& body);

    void recvAppend(faabric::transport::Message& body);

    void recvPullAppended(faabric::transport::Message& body);

    void recvClearAppended(faabric::transport::Message& body);

    void recvDelete(faabric::transport::Message& body);

    void recvLock(faabric::transport::Message& body);

    void recvUnlock(faabric::transport::Message& body);
};
}
