#pragma once

#include <atomic>

#include <faabric/flat/faabric_generated.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/snapshot/SnapshotApi.h>
#include <faabric/transport/MessageEndpointServer.h>
#include <faabric/transport/PointToPointBroker.h>

namespace faabric::snapshot {
class SnapshotServer final : public faabric::transport::MessageEndpointServer
{
  public:
    SnapshotServer();

  protected:
    void doAsyncRecv(transport::Message&& message) override;

    std::unique_ptr<google::protobuf::Message> doSyncRecv(
      transport::Message&& message) override;

    std::unique_ptr<google::protobuf::Message> recvPushSnapshot(
      const uint8_t* buffer,
      size_t bufferSize);

    std::unique_ptr<google::protobuf::Message> recvPushSnapshotUpdate(
      const uint8_t* buffer,
      size_t bufferSize);

    void recvDeleteSnapshot(const uint8_t* buffer, size_t bufferSize);

    std::unique_ptr<google::protobuf::Message> recvThreadResult(
      faabric::transport::Message&& message);

  private:
    faabric::transport::PointToPointBroker& broker;
    faabric::snapshot::SnapshotRegistry& reg;
};
}
