#include <faabric/scheduler/MpiWorldRegistry.h>
#include <faabric/scheduler/SnapshotServer.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/state/State.h>
#include <faabric/util/config.h>
#include <faabric/util/func.h>
#include <faabric/util/logging.h>

#include <faabric/proto/macros.h>
#include <grpcpp/grpcpp.h>

namespace faabric::scheduler {
SnapshotServer::SnapshotServer()
  : RPCServer(DEFAULT_RPC_HOST, SNAPSHOT_RPC_PORT)
  , scheduler(getScheduler())
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

Status PushSnapshot(
  ServerContext* context,
  const flatbuffers::grpc::Message<SnapshotPushRequest>* request,
  flatbuffers::grpc::Message<SnapshotPushResponse>* response)
{
    const SnapshotPushRequest* r = request->GetRoot();
    faabric::util::getLogger()->info(
      "Pushing shapshot {} (size {})", r->key(), r->contents().size());

    faabric::snapshot::SnapshotRegistry& reg =
      faabric::snapshot::getSnapshotRegistry();

    // TODO - set snapshot

    return Status::OK;
}
}
