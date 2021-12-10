#pragma once

#include <faabric/flat/faabric_generated.h>
#include <faabric/snapshot/SnapshotApi.h>
#include <faabric/transport/MessageEndpoint.h>
#include <faabric/transport/MessageEndpointClient.h>
#include <faabric/util/snapshot.h>

namespace faabric::snapshot {

// -----------------------------------
// Mocking
// -----------------------------------

std::vector<
  std::pair<std::string, std::shared_ptr<faabric::util::SnapshotData>>>
getSnapshotPushes();

std::vector<std::pair<std::string, std::vector<faabric::util::SnapshotDiff>>>
getSnapshotDiffPushes();

std::vector<std::pair<std::string, std::string>> getSnapshotDeletes();

std::vector<std::pair<std::string, std::pair<uint32_t, int>>>
getThreadResults();

void clearMockSnapshotRequests();

// -----------------------------------
// Client
// -----------------------------------

class SnapshotClient final : public faabric::transport::MessageEndpointClient
{
  public:
    explicit SnapshotClient(const std::string& hostIn);

    void pushSnapshot(const std::string& key,
                      int32_t groupId,
                      std::shared_ptr<faabric::util::SnapshotData> data);

    void pushSnapshotDiffs(std::string snapshotKey,
                           int32_t groupId,
                           std::vector<faabric::util::SnapshotDiff> diffs);

    void deleteSnapshot(const std::string& key);

    void pushThreadResult(uint32_t messageId, int returnValue);

  private:
    void sendHeader(faabric::snapshot::SnapshotCalls call);
};
}
