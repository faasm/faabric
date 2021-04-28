#pragma once

#include <faabric/rpc/macros.h>
#include <faabric/transport/MessageEndpoint.h>

#include <faabric/proto/mpiMessage.pb.h>

namespace faabric::transport {

class MpiMessageEndpoint final
  : public faabric::transport::MessageEndpoint
{
  public:
    MpiMessageEndpoint();

    MpiMessageEndpoint(const std::string& overrideHost);

    void sendMpiMessage(std::shared_ptr<faabric::MPIMessage> mpiMsg);

  private:
    void doHandleMessage(const void* msgData, int size) override;
};
}
