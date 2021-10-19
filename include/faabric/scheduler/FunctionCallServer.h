#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/DistributedCoordinator.h>
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

    faabric::scheduler::DistributedCoordinator& sync;

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

    // --- Function group operations ---
    void recvCoordinationLock(const uint8_t* buffer, size_t bufferSize);

    void recvCoordinationUnlock(const uint8_t* buffer, size_t bufferSize);

    void recvCoordinationNotify(const uint8_t* buffer, size_t bufferSize);

    void recvCoordinationBarrier(const uint8_t* buffer, size_t bufferSize);
};
}
