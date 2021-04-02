#pragma once

#include <faabric/flat/faabric.grpc.fb.h>
#include <faabric/flat/faabric_generated.h>
#include <faabric/rpc/RPCServer.h>
#include <faabric/scheduler/Scheduler.h>

#include <grpcpp/grpcpp.h>

using namespace grpc;

namespace faabric::scheduler {
class SnapshotServer final
  : public rpc::RPCServer
  , public SnapshotService::Service
{
  public:
    SnapshotServer();

    virtual Status PushSnapshot(
      ServerContext* context,
      const flatbuffers::grpc::Message<SnapshotPushRequest>* request,
      flatbuffers::grpc::Message<SnapshotPushResponse>* response) override;

  protected:
    void doStart(const std::string& serverAddr) override;
};
}
