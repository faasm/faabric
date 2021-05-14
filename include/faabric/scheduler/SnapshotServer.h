#pragma once

#include <faabric/flat/faabric_generated.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/scheduler/SnapshotApi.h>
#include <faabric/transport/MessageEndpointServer.h>

namespace faabric::scheduler {
class SnapshotServer final : public faabric::transport::MessageEndpointServer
{
  public:
    SnapshotServer();

  protected:
    void doRecv(const void* headerData,
                int headerSize,
                const void* bodyData,
                int bodySize) override;

    /* Snapshot server API */

    void recvPushSnapshot(const void* msgData, int size);

    void recvDeleteSnapshot(const void* msgData, int size);
};
}
