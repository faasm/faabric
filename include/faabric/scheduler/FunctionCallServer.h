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

    void doAsyncRecv(int header,
                     const uint8_t* buffer,
                     size_t bufferSize) override;

    std::unique_ptr<google::protobuf::Message>
    doSyncRecv(int header, const uint8_t* buffer, size_t bufferSize) override;

    std::unique_ptr<google::protobuf::Message> recvFlush(const uint8_t* buffer,
                                                         size_t bufferSize);

    std::unique_ptr<google::protobuf::Message> recvGetResources(
      const uint8_t* buffer,
      size_t bufferSize);

    void recvExecuteFunctions(const uint8_t* buffer, size_t bufferSize);

    void recvUnregister(const uint8_t* buffer, size_t bufferSize);

    // Function group operations
    void recvFunctionGroupLock(const uint8_t* buffer, size_t bufferSize);

    void recvFunctionGroupUnlock(const uint8_t* buffer, size_t bufferSize);

    void recvFunctionGroupNotify(const uint8_t* buffer, size_t bufferSize);

    void recvFunctionGroupBarrier(const uint8_t* buffer, size_t bufferSize);
};
}
