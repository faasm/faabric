#pragma once

#include <faabric/flat/faabric_generated.h>
#include <faabric/scheduler/DistributedCoordinator.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/snapshot/SnapshotApi.h>
#include <faabric/transport/MessageEndpointServer.h>

namespace faabric::snapshot {
class SnapshotServer final : public faabric::transport::MessageEndpointServer
{
  public:
    SnapshotServer();

  protected:
    void doAsyncRecv(int header,
                     const uint8_t* buffer,
                     size_t bufferSize) override;

    std::unique_ptr<google::protobuf::Message>
    doSyncRecv(int header, const uint8_t* buffer, size_t bufferSize) override;

    std::unique_ptr<google::protobuf::Message> recvPushSnapshot(
      const uint8_t* buffer,
      size_t bufferSize);

    std::unique_ptr<google::protobuf::Message> recvPushSnapshotDiffs(
      const uint8_t* buffer,
      size_t bufferSize);

    void recvDeleteSnapshot(const uint8_t* buffer, size_t bufferSize);

    void recvThreadResult(const uint8_t* buffer, size_t bufferSize);

  private:
    faabric::scheduler::DistributedCoordinator& distCoord;
};
}
