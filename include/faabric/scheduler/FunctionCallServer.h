#pragma once

#include <faabric/proto/faabric.grpc.pb.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/MpiWorld.h>
#include <faabric/scheduler/MpiWorldRegistry.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/transport/MessageEndpointServer.h>

namespace faabric::scheduler {
enum FunctionCalls
{
    MpiMessage = 1,
    ExecuteFunctions = 2,
    Flush = 3,
    Unregister = 4,
};

class FunctionCallServer final
  : public faabric::transport::MessageEndpointServer
{
  public:
    FunctionCallServer();

    /*
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
    */

  private:
    Scheduler& scheduler;
    faabric::scheduler::FunctionCalls lastHeader;

    void doRecv(const void* msgData, int size) override;

    void recvMpiMessage(const void* msgData, int size);
};
}
