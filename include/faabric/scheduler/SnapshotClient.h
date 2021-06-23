#pragma once

#include <faabric/flat/faabric_generated.h>
#include <faabric/scheduler/SnapshotApi.h>
#include <faabric/transport/MessageEndpoint.h>
#include <faabric/util/snapshot.h>

namespace faabric::scheduler {

// -----------------------------------
// Mocking
// -----------------------------------

std::vector<std::pair<std::string, faabric::util::SnapshotData>>
getSnapshotPushes();

std::vector<std::pair<std::string, std::vector<faabric::util::SnapshotDiff>>>
getSnapshotDiffPushes();

std::vector<std::pair<std::string, std::string>> getSnapshotDeletes();

std::vector<std::pair<std::string,
                      std::tuple<uint32_t,
                                 int,
                                 std::string,
                                 std::vector<faabric::util::SnapshotDiff>>>>
getThreadResults();

void clearMockSnapshotRequests();

// -----------------------------------
// gRPC client
// -----------------------------------

class SnapshotClient final : public faabric::transport::SendMessageEndpoint
{
  public:
    explicit SnapshotClient(const std::string& hostIn);

    /* Snapshot client external API */

    void pushSnapshot(const std::string& key,
                      const faabric::util::SnapshotData& data);

    void pushSnapshotDiffs(std::string snapshotKey,
                           std::vector<faabric::util::SnapshotDiff> diffs);

    void deleteSnapshot(const std::string& key);

    void pushThreadResult(uint32_t messageId, int returnValue);

    void pushThreadResult(
      uint32_t messageId,
      int returnValue,
      const std::string& snapshotKey,
      const std::vector<faabric::util::SnapshotDiff>& diffs);

  private:
    void sendHeader(faabric::scheduler::SnapshotCalls call);
};
}
