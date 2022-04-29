#include <faabric/transport/MessageEndpointClient.h>

namespace faabric::transport {

MessageEndpointClient::MessageEndpointClient(std::string hostIn,
                                             int asyncPortIn,
                                             int syncPortIn,
                                             int timeoutMs)
  : host(hostIn)
  , asyncPort(asyncPortIn)
  , syncPort(syncPortIn)
  , asyncEndpoint(host, asyncPort, timeoutMs)
  , syncEndpoint(host, syncPort, timeoutMs)
{}

void MessageEndpointClient::asyncSend(int header,
                                      google::protobuf::Message* msg,
                                      int sequenceNum)
{
    size_t msgSize = msg->ByteSizeLong();
    uint8_t buffer[msgSize];

    if (!msg->SerializeToArray(buffer, msgSize)) {
        throw std::runtime_error("Error serialising message");
    }

    asyncSend(header, buffer, msgSize, sequenceNum);
}

void MessageEndpointClient::asyncSend(int header,
                                      const uint8_t* buffer,
                                      size_t bufferSize,
                                      int sequenceNum)
{
    asyncEndpoint.send(header, buffer, bufferSize, sequenceNum);
}

void MessageEndpointClient::syncSend(int header,
                                     google::protobuf::Message* msg,
                                     google::protobuf::Message* response)
{
    size_t msgSize = msg->ByteSizeLong();
    uint8_t buffer[msgSize];
    if (!msg->SerializeToArray(buffer, msgSize)) {
        throw std::runtime_error("Error serialising message");
    }

    syncSend(header, buffer, msgSize, response);
}

void MessageEndpointClient::syncSend(int header,
                                     const uint8_t* buffer,
                                     const size_t bufferSize,
                                     google::protobuf::Message* response)
{
    Message responseMsg =
      syncEndpoint.sendAwaitResponse(header, buffer, bufferSize);

    // Deserialise response
    if (!response->ParseFromArray(responseMsg.data(), responseMsg.size())) {
        throw std::runtime_error("Error deserialising message");
    }
}
}
