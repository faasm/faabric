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

namespace {
thread_local std::vector<uint8_t> msgBuffer;
}

void MessageEndpointClient::asyncSend(int header,
                                      google::protobuf::Message* msg)
{
    size_t msgSize = msg->ByteSizeLong();
    msgBuffer.resize(msgSize);

    if (!msg->SerializeToArray(msgBuffer.data(), msgBuffer.size())) {
        throw std::runtime_error("Error serialising message");
    }

    asyncSend(header, msgBuffer.data(), msgBuffer.size());
}

void MessageEndpointClient::asyncSend(int header,
                                      const uint8_t* buffer,
                                      size_t bufferSize)
{
    asyncEndpoint.sendHeader(header);

    asyncEndpoint.send(buffer, bufferSize);
}

void MessageEndpointClient::syncSend(int header,
                                     google::protobuf::Message* msg,
                                     google::protobuf::Message* response)
{
    size_t msgSize = msg->ByteSizeLong();
    msgBuffer.resize(msgSize);
    if (!msg->SerializeToArray(msgBuffer.data(), msgBuffer.size())) {
        throw std::runtime_error("Error serialising message");
    }

    syncSend(header, msgBuffer.data(), msgBuffer.size(), response);
}

void MessageEndpointClient::syncSend(int header,
                                     const uint8_t* buffer,
                                     const size_t bufferSize,
                                     google::protobuf::Message* response)
{
    syncEndpoint.sendHeader(header);

    Message responseMsg = syncEndpoint.sendAwaitResponse(buffer, bufferSize);

    // Deserialise response
    if (!response->ParseFromArray(responseMsg.data(), responseMsg.size())) {
        throw std::runtime_error("Error deserialising message");
    }
}
}
