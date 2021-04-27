#pragma once

#include <faabric/rpc/macros.h>
#include <faabric/zeromq/MessageEndpoint.h>

#include <faabric/zeromq/mpiMessage.pb.h>

namespace faabric::zeromq {

class MpiMessageEndpoint final
  : public faabric::zeromq::MessageEndpoint
{
  public:
    MpiMessageEndpoint();

    MpiMessageEndpoint(const std::string& overrideHost);

    void sendMpiMessage(std::shared_ptr<faabric::MPIMessage> mpiMsg);

  private:
    void doHandleMessage(zmq::message_t& msg) override;
};
}
