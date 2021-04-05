#include <faabric/scheduler/SnapshotClient.h>

#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include <faabric/rpc/macros.h>
#include <faabric/util/logging.h>
#include <faabric/util/queue.h>
#include <faabric/util/testing.h>

namespace faabric::scheduler {

// -----------------------------------
// Mocking
// -----------------------------------

static std::vector<std::pair<std::string, faabric::util::SnapshotData>>
  snapshotPushes;

static std::vector<std::pair<std::string, std::string>> snapshotDeletes;

std::vector<std::pair<std::string, faabric::util::SnapshotData>>
getSnapshotPushes()
{
    return snapshotPushes;
}

std::vector<std::pair<std::string, std::string>> getSnapshotDeletes()
{
    return snapshotDeletes;
}

void clearMockSnapshotRequests()
{
    snapshotPushes.clear();
    snapshotDeletes.clear();
}

// -----------------------------------
// gRPC client
// -----------------------------------

SnapshotClient::SnapshotClient(const std::string& hostIn)
  : host(hostIn)
  , channel(grpc::CreateChannel(host + ":" + std::to_string(SNAPSHOT_RPC_PORT),
                                grpc::InsecureChannelCredentials()))
  , stub(SnapshotService::NewStub(channel))
{}

void SnapshotClient::pushSnapshot(const std::string& key,
                                  const faabric::util::SnapshotData& req)
{
    auto logger = faabric::util::getLogger();

    if (faabric::util::isMockMode()) {
        snapshotPushes.emplace_back(host, req);
    } else {
        logger->debug("Pushing snapshot {} to {}", key, host);

        ClientContext context;

        // TODO - avoid copying data here
        flatbuffers::grpc::MessageBuilder mb;
        auto keyOffset = mb.CreateString(key);
        auto dataOffset = mb.CreateVector<uint8_t>(req.data, req.size);
        auto requestOffset =
          CreateSnapshotPushRequest(mb, keyOffset, dataOffset);

        mb.Finish(requestOffset);
        auto requestMsg = mb.ReleaseMessage<SnapshotPushRequest>();

        flatbuffers::grpc::Message<SnapshotPushResponse> responseMsg;
        CHECK_RPC("snapshotPush",
                  stub->PushSnapshot(&context, requestMsg, &responseMsg));
    }
}

void SnapshotClient::deleteSnapshot(const std::string& key)
{
    auto logger = faabric::util::getLogger();

    if (faabric::util::isMockMode()) {
        snapshotDeletes.emplace_back(host, key);
    } else {
        logger->debug("Deleting snapshot {}", key);

        ClientContext context;

        // TODO - avoid copying data here
        flatbuffers::grpc::MessageBuilder mb;
        auto keyOffset = mb.CreateString(key);
        auto requestOffset = CreateSnapshotDeleteRequest(mb, keyOffset);
        mb.Finish(requestOffset);

        auto requestMsg = mb.ReleaseMessage<SnapshotDeleteRequest>();

        flatbuffers::grpc::Message<SnapshotDeleteResponse> responseMsg;
        CHECK_RPC("snapshotDelete",
                  stub->DeleteSnapshot(&context, requestMsg, &responseMsg));
    }
}
}
