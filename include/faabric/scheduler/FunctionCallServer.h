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

  private:
    Scheduler& scheduler;

    void doAsyncRecv(faabric::transport::Message& header,
                     faabric::transport::Message& body) override;

    std::unique_ptr<google::protobuf::Message> doSyncRecv(
      faabric::transport::Message& header,
      faabric::transport::Message& body) override;

    std::unique_ptr<google::protobuf::Message> recvFlush(
      faabric::transport::Message& body);

    std::unique_ptr<google::protobuf::Message> recvGetResources(
      faabric::transport::Message& body);

    void recvExecuteFunctions(faabric::transport::Message& body);

    void recvUnregister(faabric::transport::Message& body);
};
}
