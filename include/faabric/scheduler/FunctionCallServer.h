#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/MpiWorld.h>
#include <faabric/scheduler/MpiWorldRegistry.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/transport/MessageEndpointServer.h>
#include <faabric/transport/SimpleMessageEndpoint.h>

namespace faabric::scheduler {
class FunctionCallServer final
  : public faabric::transport::MessageEndpointServer
{
  public:
    FunctionCallServer();

    void stop();

  private:
    Scheduler& scheduler;

    void doRecv(const void* headerData,
                int headerSize,
                const void* bodyData,
                int bodySize) override;

    void sendEmptyResponse(const std::string& returnHost);

    void recvMpiMessage(const void* msgData, int size);

    void recvFlush(const void* msgData, int size);

    void recvExecuteFunctions(const void* msgData, int size);

    void recvGetResources(const void* msgData, int size);

    void recvUnregister(const void* msgData, int size);
};
}
