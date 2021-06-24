#include <faabric/transport/MessageEndpointClient.h>

namespace faabric::transport {

MessageEndpointClient::MessageEndpointClient(std::string hostIn, int portIn)
  : host(hostIn)
  , asyncPort(portIn)
  , syncPort(portIn + 1)
  , asyncEndpoint(host, asyncPort)
  , syncEndpoint(host, syncPort)
{}

void MessageEndpointClient::asyncSend(int header,
                                      google::protobuf::Message* msg)
{
    size_t msgSize = msg->ByteSizeLong();
    uint8_t sMsg[msgSize];

    if (!msg->SerializeToArray(sMsg, msgSize)) {
        throw std::runtime_error("Error serialising message");
    }

    asyncSend(header, sMsg, msgSize);
}

void MessageEndpointClient::asyncSend(int header, uint8_t* buffer, size_t bufferSize) {
    syncEndpoint.sendHeader(header);

    asyncEndpoint.send(buffer, bufferSize);
}

void MessageEndpointClient::syncSend(int header,
                                     google::protobuf::Message* msg,
                                     google::protobuf::Message* response)
{
    size_t msgSize = msg->ByteSizeLong();
    uint8_t sMsg[msgSize];
    if (!msg->SerializeToArray(sMsg, msgSize)) {
        throw std::runtime_error("Error serialising message");
    }

    syncSend(header, sMsg, msgSize, response);
}

void MessageEndpointClient::syncSend(int header,
                                     const uint8_t* buffer,
                                     const size_t bufferSize,
                                     google::protobuf::Message* response)
{
    syncEndpoint.sendHeader(header);

    Message responseMsg = syncEndpoint.sendAwaitResponse(buffer, bufferSize);
    // Deserialise message string
    if (!response->ParseFromArray(responseMsg.data(), responseMsg.size())) {
        throw std::runtime_error("Error deserialising message");
    }
}
}
