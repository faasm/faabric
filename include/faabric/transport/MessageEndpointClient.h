#pragma once

#include <faabric/flat/faabric_generated.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/transport/Message.h>
#include <faabric/transport/MessageEndpoint.h>

namespace faabric::transport {
class MessageEndpointClient
{
  public:
    MessageEndpointClient(std::string hostIn,
                          int asyncPort,
                          int syncPort,
                          int timeoutMs = DEFAULT_SOCKET_TIMEOUT_MS);

    void asyncSend(int header,
                   google::protobuf::Message* msg,
                   int sequenceNum = NO_SEQUENCE_NUM);

    void asyncSend(int header,
                   const uint8_t* buffer,
                   size_t bufferSize,
                   int sequenceNum = NO_SEQUENCE_NUM);

    void syncSend(int header,
                  google::protobuf::Message* msg,
                  google::protobuf::Message* response);

    void syncSend(int header,
                  const uint8_t* buffer,
                  size_t bufferSize,
                  google::protobuf::Message* response);

  protected:
    const std::string host;

    const int asyncPort;

    const int syncPort;

    faabric::transport::AsyncSendMessageEndpoint asyncEndpoint;

    faabric::transport::SyncSendMessageEndpoint syncEndpoint;
};
}
