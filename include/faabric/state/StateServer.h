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

    std::unique_ptr<google::protobuf::Message> doSyncRecv(
      faabric::transport::Message& header,
      faabric::transport::Message& body) override;

    // Sync methods

    std::unique_ptr<google::protobuf::Message> recvSize(
      faabric::transport::Message& body);

    std::unique_ptr<google::protobuf::Message> recvPull(
      faabric::transport::Message& body);

    std::unique_ptr<google::protobuf::Message> recvPush(
      faabric::transport::Message& body);

    std::unique_ptr<google::protobuf::Message> recvAppend(
      faabric::transport::Message& body);

    std::unique_ptr<google::protobuf::Message> recvPullAppended(
      faabric::transport::Message& body);

    std::unique_ptr<google::protobuf::Message> recvClearAppended(
      faabric::transport::Message& body);

    std::unique_ptr<google::protobuf::Message> recvDelete(
      faabric::transport::Message& body);

    std::unique_ptr<google::protobuf::Message> recvLock(
      faabric::transport::Message& body);

    std::unique_ptr<google::protobuf::Message> recvUnlock(
      faabric::transport::Message& body);
};
}
