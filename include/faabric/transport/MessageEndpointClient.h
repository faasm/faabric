#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/transport/Message.h>
#include <faabric/transport/MessageEndpoint.h>

namespace faabric::transport {
class MessageEndpointClient
{
  public:
    MessageEndpointClient(std::string hostIn, int portIn);

  protected:
    const std::string host;

    void asyncSend(int header, std::unique_ptr<google::protobuf::Message> msg);

    void syncSend(int header,
                  std::unique_ptr<google::protobuf::Message> msg,
                  std::unique_ptr<google::protobuf::Message> response);

  private:
    const int asyncPort;

    const int syncPort;

    faabric::transport::AsyncSendMessageEndpoint asyncEndpoint;

    faabric::transport::SyncSendMessageEndpoint syncEndpoint;
};
}
