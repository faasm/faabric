#pragma once

#include <faabric/transport/MessageEndpointClient.h>

namespace faabric::transport {

MessageEndpointClient::MessageEndpointClient(std::string hostIn, int portIn)
  : host(hostIn)
  , asyncPort(portIn)
  , syncPort(portIn + 1)
  , asyncEndpoint(host, asyncPort)
  , syncEndpoint(host, asyncPort)
{}

void MessageEndpointClient::asyncSend(
  int header,
  std::unique_ptr<google::protobuf::Message> msg)
{}

void MessageEndpointClient::syncSend(
  int header,
  std::unique_ptr<google::protobuf::Message> msg,
  std::unique_ptr<google::protobuf::Message> response)
{
    syncEndpoint.sendHeader(header);

    size_t msgSize = msg->ByteSizeLong();
    uint8_t sMsg[msgSize];
    if (!msg->SerializeToArray(sMsg, msgSize)) {
        throw std::runtime_error("Error serialising message");
    }
    Message responseMsg = syncEndpoint.sendAwaitResponse(sMsg, msgSize);
    // Deserialise message string
    if (!response->ParseFromArray(responseMsg.data(), responseMsg.size())) {
        throw std::runtime_error("Error deserialising message");
    }
}
}
