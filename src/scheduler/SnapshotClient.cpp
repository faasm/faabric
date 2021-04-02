#include <faabric/scheduler/SnapshotClient.h>

#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include <faabric/proto/macros.h>
#include <faabric/util/queue.h>
#include <faabric/util/testing.h>

namespace faabric::scheduler {

// -----------------------------------
// Mocking
// -----------------------------------

static std::vector<std::pair<std::string, faabric::util::SnapshotData>>
  snapshotPushes;

std::vector<std::pair<std::string, faabric::util::SnapshotData>>
getSnapshotPushes()
{
    return snapshotPushes;
}

void clearMockSnapshotRequests()
{
    snapshotPushes.clear();
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

void SnapshotClient::pushSnapshot(const std::string &key, const faabric::util::SnapshotData& req)
{
    if (faabric::util::isMockMode()) {
        snapshotPushes.emplace_back(host, req);
    } else {
        ClientContext context;
        flatbuffers::grpc::MessageBuilder mb;
        auto keyOffset = mb.CreateString(key);
        auto dataOffset = mb.CreateVector<uint8_t>(req.data, req.size);
        auto requestOffset = CreateSnapshotPushRequest(mb, keyOffset, dataOffset);

        mb.Finish(requestOffset);
        auto requestMsg = mb.ReleaseMessage<SnapshotPushRequest>();

        flatbuffers::grpc::Message<SnapshotPushResponse> responseMsg;
        CHECK_RPC("snapshotPush",
                  stub->PushSnapshot(&context, requestMsg, &responseMsg));
    }
}
}
