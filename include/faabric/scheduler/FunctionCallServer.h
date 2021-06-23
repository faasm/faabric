#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/FunctionCallApi.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/transport/MessageEndpointServer.h>

namespace faabric::scheduler {
class FunctionCallServer final
  : public faabric::transport::MessageEndpointServer
{
  public:
    FunctionCallServer();

    void stop() override;

  private:
    Scheduler& scheduler;

    void doRecv(faabric::transport::Message& header,
                faabric::transport::Message& body) override;

    /* Function call server API */

    void recvFlush(faabric::transport::Message& body);

    void recvExecuteFunctions(faabric::transport::Message& body);

    void recvGetResources(faabric::transport::Message& body);

    void recvUnregister(faabric::transport::Message& body);

    void recvSetThreadResult(faabric::transport::Message& body);
};
}
