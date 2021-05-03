#pragma once

#include <faabric/proto/faabric.grpc.pb.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/MpiWorld.h>
#include <faabric/scheduler/MpiWorldRegistry.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/transport/MessageEndpointServer.h>
#include <faabric/transport/SimpleMessageEndpoint.h>

#define DEFAULT_RPC_HOST "0.0.0.0"
#define STATE_PORT 8003
#define FUNCTION_CALL_PORT 8004
#define MPI_MESSAGE_PORT 8005
#define SNAPSHOT_RPC_PORT 8006
#define REPLY_PORT_OFFSET 100

namespace faabric::scheduler {
enum FunctionCalls
{
    None = 0,
    MpiMessage = 1,
    ExecuteFunctions = 2,
    Flush = 3,
    Unregister = 4,
    GetResources = 5,
};

class FunctionCallServer final
  : public faabric::transport::MessageEndpointServer
{
  public:
    FunctionCallServer();

    /*
    Status GetResources(ServerContext* context,
                        const faabric::ResourceRequest* request,
                        faabric::HostResources* response) override;
    */

  private:
    Scheduler& scheduler;

    void doRecv(void* msgData, int size) override;

    void doRecv(const void* headerData,
                int headerSize,
                const void* bodyData,
                int bodySize) override;

    void recvMpiMessage(const void* msgData, int size);

    void recvFlush();

    void recvExecuteFunctions(const void* msgData, int size);

    void recvGetResources(const void* msgData, int size);

    void recvUnregister(const void* msgData, int size);
};
}
