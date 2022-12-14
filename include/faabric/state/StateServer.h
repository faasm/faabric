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

    void logOperation(const std::string& op);

    void doAsyncRecv(transport::Message& message) override;

    std::unique_ptr<google::protobuf::Message> doSyncRecv(
      transport::Message& message) override;

    // Sync methods

    std::unique_ptr<google::protobuf::Message> recvSize(
      std::span<const uint8_t> buffer);

    std::unique_ptr<google::protobuf::Message> recvPull(
      std::span<const uint8_t> buffer);

    std::unique_ptr<google::protobuf::Message> recvPush(
      std::span<const uint8_t> buffer);

    std::unique_ptr<google::protobuf::Message> recvAppend(
      std::span<const uint8_t> buffer);

    std::unique_ptr<google::protobuf::Message> recvPullAppended(
      std::span<const uint8_t> buffer);

    std::unique_ptr<google::protobuf::Message> recvClearAppended(
      std::span<const uint8_t> buffer);

    std::unique_ptr<google::protobuf::Message> recvDelete(
      std::span<const uint8_t> buffer);
};
}
