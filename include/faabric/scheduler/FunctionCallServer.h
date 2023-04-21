#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/FunctionCallApi.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/transport/MessageEndpointServer.h>

namespace faabric::scheduler {
class FunctionCallServer final
  : public faabric::transport::MessageEndpointServer
{
  public:
    FunctionCallServer();

  private:
    Scheduler& scheduler;

    void doAsyncRecv(transport::Message& message) override;

    std::unique_ptr<google::protobuf::Message> doSyncRecv(
      transport::Message& message) override;

    std::unique_ptr<google::protobuf::Message> recvFlush(
      std::span<const uint8_t> buffer);

    std::unique_ptr<google::protobuf::Message> recvGetResources(
      std::span<const uint8_t> buffer);

    void recvExecuteFunctions(std::span<const uint8_t> buffer);

    // TODO: do we delete this one?
    // void recvUnregister(std::span<const uint8_t> buffer);
};
}
