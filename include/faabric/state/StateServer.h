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

    void logOperation(const std::string &op);

    void doAsyncRecv(int header,
                     const uint8_t* buffer,
                     size_t bufferSize) override;

    std::unique_ptr<google::protobuf::Message>
    doSyncRecv(int header, const uint8_t* buffer, size_t bufferSize) override;

    // Sync methods

    std::unique_ptr<google::protobuf::Message> recvSize(const uint8_t* buffer,
                                                        size_t bufferSize);

    std::unique_ptr<google::protobuf::Message> recvPull(const uint8_t* buffer,
                                                        size_t bufferSize);

    std::unique_ptr<google::protobuf::Message> recvPush(const uint8_t* buffer,
                                                        size_t bufferSize);

    std::unique_ptr<google::protobuf::Message> recvAppend(const uint8_t* buffer,
                                                          size_t bufferSize);

    std::unique_ptr<google::protobuf::Message> recvPullAppended(
      const uint8_t* buffer,
      size_t bufferSize);

    std::unique_ptr<google::protobuf::Message> recvClearAppended(
      const uint8_t* buffer,
      size_t bufferSize);

    std::unique_ptr<google::protobuf::Message> recvDelete(const uint8_t* buffer,
                                                          size_t bufferSize);

    std::unique_ptr<google::protobuf::Message> recvLock(const uint8_t* buffer,
                                                        size_t bufferSize);

    std::unique_ptr<google::protobuf::Message> recvUnlock(const uint8_t* buffer,
                                                          size_t bufferSize);
};
}
