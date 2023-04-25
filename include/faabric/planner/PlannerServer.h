#pragma once

#include <faabric/planner/Planner.h>
#include <faabric/transport/MessageEndpointServer.h>

namespace faabric::planner {
class PlannerServer final : public faabric::transport::MessageEndpointServer
{
  public:
    PlannerServer();

  protected:
    void doAsyncRecv(transport::Message& message) override;

    std::unique_ptr<google::protobuf::Message> doSyncRecv(
      transport::Message& message) override;

    // Synchronous calls

    std::unique_ptr<google::protobuf::Message> recvPing();

    std::unique_ptr<google::protobuf::Message> recvSetTestsConfig(
      std::span<const uint8_t> buffer);

    std::unique_ptr<google::protobuf::Message> recvGetAvailableHosts();

    std::unique_ptr<google::protobuf::Message> recvRegisterHost(
      std::span<const uint8_t> buffer);

    std::unique_ptr<google::protobuf::Message> recvRemoveHost(
      std::span<const uint8_t> buffer);

    std::unique_ptr<google::protobuf::Message> recvCallFunctions(
      std::span<const uint8_t> buffer);

    std::unique_ptr<google::protobuf::Message> recvGetSchedulingDecision(
      std::span<const uint8_t> buffer);

    std::unique_ptr<google::protobuf::Message> recvSetMessageResult(
      std::span<const uint8_t> buffer);

    std::unique_ptr<google::protobuf::Message> recvGetMessageResult(
      std::span<const uint8_t> buffer);

    std::unique_ptr<google::protobuf::Message> recvGetBatchResult(
      std::span<const uint8_t> buffer);

  private:
    faabric::planner::Planner& planner;
};
}
