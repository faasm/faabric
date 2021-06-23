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
    void doRecv(faabric::transport::Message& header,
                faabric::transport::Message& body) override;

    /* Snapshot server API */

    void recvPushSnapshot(faabric::transport::Message& msg);

    void recvDeleteSnapshot(faabric::transport::Message& msg);

    void recvPushSnapshotDiffs(faabric::transport::Message& msg);

    void recvThreadResult(faabric::transport::Message& msg);

  private:
    void applyDiffsToSnapshot(
      const std::string& snapshotKey,
      const flatbuffers::Vector<flatbuffers::Offset<SnapshotDiffChunk>>* diffs);
};
}
