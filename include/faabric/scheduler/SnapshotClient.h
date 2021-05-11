#pragma once

#include <faabric/flat/faabric_generated.h>
#include <faabric/transport/MessageEndpointClient.h>
#include <faabric/util/snapshot.h>

namespace faabric::scheduler {

// -----------------------------------
// Mocking
// -----------------------------------

std::vector<std::pair<std::string, faabric::util::SnapshotData>>
getSnapshotPushes();

std::vector<std::pair<std::string, std::string>> getSnapshotDeletes();

void clearMockSnapshotRequests();

// -----------------------------------
// gRPC client
// -----------------------------------

class SnapshotClient final : public faabric::transport::MessageEndpointClient
{
  public:
    explicit SnapshotClient(const std::string& hostIn);

    ~SnapshotClient();

    /* Snapshot client external API */

    void pushSnapshot(const std::string& key,
                      const faabric::util::SnapshotData& data);

    void deleteSnapshot(const std::string& key);

  private:
    void sendHeader(faabric::scheduler::SnapshotCalls call);
};
}
