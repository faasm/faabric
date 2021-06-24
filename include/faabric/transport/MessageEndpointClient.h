#pragma once

#include <faabric/flat/faabric_generated.h>
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

    void asyncSend(int header, google::protobuf::Message* msg);

    void asyncSend(int header, uint8_t* buffer, size_t bufferSize);

    void syncSend(int header,
                  google::protobuf::Message* msg,
                  google::protobuf::Message* response);

    void syncSend(int header,
                  const uint8_t* buffer,
                  size_t bufferSize,
                  google::protobuf::Message* response);

  private:
    const int asyncPort;

    const int syncPort;

    faabric::transport::AsyncSendMessageEndpoint asyncEndpoint;

    faabric::transport::SyncSendMessageEndpoint syncEndpoint;
};
}
