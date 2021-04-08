#include <faabric/scheduler/MpiWorldRegistry.h>
#include <faabric/scheduler/SnapshotServer.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/state/State.h>
#include <faabric/util/config.h>
#include <faabric/util/func.h>
#include <faabric/util/logging.h>

#include <faabric/rpc/macros.h>
#include <grpcpp/grpcpp.h>

namespace faabric::scheduler {
SnapshotServer::SnapshotServer()
  : RPCServer(DEFAULT_RPC_HOST, SNAPSHOT_RPC_PORT)
{}

void SnapshotServer::doStart(const std::string& serverAddr)
{
    // Build the server
    ServerBuilder builder;
    builder.AddListeningPort(serverAddr, InsecureServerCredentials());
    builder.RegisterService(this);

    // Start it
    server = builder.BuildAndStart();
    faabric::util::getLogger()->info("Snapshot server listening on {}",
                                     serverAddr);

    server->Wait();
}

Status SnapshotServer::PushSnapshot(
  ServerContext* context,
  const flatbuffers::grpc::Message<SnapshotPushRequest>* request,
  flatbuffers::grpc::Message<SnapshotPushResponse>* response)
{
    const SnapshotPushRequest* r = request->GetRoot();
    faabric::util::getLogger()->info("Pushing shapshot {} (size {})",
                                     r->key()->c_str(),
                                     r->contents()->size());

    faabric::snapshot::SnapshotRegistry& reg =
      faabric::snapshot::getSnapshotRegistry();

    // Set up the snapshot
    faabric::util::SnapshotData data;
    data.size = r->contents()->size();
    data.data = r->contents()->Data();
    reg.takeSnapshot(r->key()->str(), data, true);

    flatbuffers::grpc::MessageBuilder mb;
    auto messageOffset = mb.CreateString("Success");
    auto responseOffset = CreateSnapshotPushResponse(mb, messageOffset);
    mb.Finish(responseOffset);
    *response = mb.ReleaseMessage<SnapshotPushResponse>();

    return Status::OK;
}

Status SnapshotServer::DeleteSnapshot(
  ServerContext* context,
  const flatbuffers::grpc::Message<SnapshotDeleteRequest>* request,
  flatbuffers::grpc::Message<SnapshotDeleteResponse>* response)
{
    const SnapshotDeleteRequest* r = request->GetRoot();
    faabric::util::getLogger()->info("Deleting shapshot {}", r->key()->c_str());

    faabric::snapshot::SnapshotRegistry& reg =
      faabric::snapshot::getSnapshotRegistry();

    reg.deleteSnapshot(r->key()->str());

    flatbuffers::grpc::MessageBuilder mb;
    auto messageOffset = mb.CreateString("Success");
    auto responseOffset = CreateSnapshotPushResponse(mb, messageOffset);
    mb.Finish(responseOffset);
    *response = mb.ReleaseMessage<SnapshotDeleteResponse>();

    return Status::OK;
}
}
