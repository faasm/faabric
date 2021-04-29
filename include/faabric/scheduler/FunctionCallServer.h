#pragma once

#include <faabric/scheduler/Scheduler.h>

#include <faabric/proto/faabric.grpc.pb.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/rpc/RPCServer.h>

using namespace grpc;

namespace faabric::scheduler {
class FunctionCallServer final
  : public rpc::RPCServer
  , public faabric::FunctionRPCService::Service
{
  public:
    FunctionCallServer();

    Status Flush(ServerContext* context,
                 const faabric::Message* request,
                 faabric::FunctionStatusResponse* response) override;

    Status MPICall(ServerContext* context,
                   const faabric::MPIMessage* request,
                   faabric::FunctionStatusResponse* response) override;

    Status GetResources(ServerContext* context,
                        const faabric::ResourceRequest* request,
                        faabric::HostResources* response) override;

    Status ExecuteFunctions(ServerContext* context,
                            const faabric::BatchExecuteRequest* request,
                            faabric::FunctionStatusResponse* response) override;

    Status Unregister(ServerContext* context,
                      const faabric::UnregisterRequest* request,
                      faabric::FunctionStatusResponse* response) override;

    Status SetThreadResult(ServerContext* context,
                           const faabric::ThreadResultRequest* request,
                           faabric::FunctionStatusResponse* response) override;

  protected:
    void doStart(const std::string& serverAddr) override;
};
}
