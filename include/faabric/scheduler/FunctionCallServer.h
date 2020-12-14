#pragma once

#include <faabric/scheduler/Scheduler.h>

#include <faabric/proto/RPCServer.h>
#include <faabric/proto/faabric.grpc.pb.h>
#include <faabric/proto/faabric.pb.h>

using namespace grpc;

namespace faabric::scheduler {
class FunctionCallServer final
  : public rpc::RPCServer
  , public faabric::FunctionRPCService::Service
{
  public:
    FunctionCallServer();

    Status ShareFunction(ServerContext* context,
                         const faabric::Message* request,
                         faabric::FunctionStatusResponse* response) override;

    Status MPICall(ServerContext* context,
                   const faabric::MPIMessage* request,
                   faabric::FunctionStatusResponse* response) override;

  protected:
    void doStart(const std::string& serverAddr) override;

  private:
    Scheduler& scheduler;
};
}
