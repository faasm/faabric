#include <faabric/transport/MessageEndpointClient.h>
#include <faabric/util/testing.h>
#include <optional>

namespace faabric::transport {

MessageEndpointClient::MessageEndpointClient(std::string hostIn,
                                             int asyncPortIn,
                                             int syncPortIn,
                                             int timeoutMs)
  : host(hostIn)
  , asyncPort(asyncPortIn)
  , syncPort(syncPortIn)
  , asyncEndpoint(std::nullopt)
  , syncEndpoint(std::nullopt)
{
    if (!faabric::util::isMockMode()) {
        asyncEndpoint.emplace(host, asyncPort, timeoutMs);
        syncEndpoint.emplace(host, syncPort, timeoutMs);
    }
}

void MessageEndpointClient::asyncSend(int header,
                                      google::protobuf::Message* msg,
                                      int sequenceNum)
{
    std::string buffer;

    if (!msg->SerializeToString(&buffer)) {
        throw std::runtime_error("Error serialising message");
    }

    asyncSend(header,
              reinterpret_cast<uint8_t*>(buffer.data()),
              buffer.size(),
              sequenceNum);
}

// TODO: consider making an iovec-style scatter/gather alternative signature
void MessageEndpointClient::asyncSend(int header,
                                      const uint8_t* buffer,
                                      size_t bufferSize,
                                      int sequenceNum)
{
    if (asyncEndpoint.has_value()) {
        asyncEndpoint->send(header, buffer, bufferSize, sequenceNum);
    }
}

void MessageEndpointClient::syncSend(int header,
                                     google::protobuf::Message* msg,
                                     google::protobuf::Message* response)
{
    std::string buffer;
    if (!msg->SerializeToString(&buffer)) {
        throw std::runtime_error("Error serialising message");
    }

    syncSend(header,
             reinterpret_cast<uint8_t*>(buffer.data()),
             buffer.size(),
             response);
}

void MessageEndpointClient::syncSend(int header,
                                     const uint8_t* buffer,
                                     const size_t bufferSize,
                                     google::protobuf::Message* response)
{
    if (syncEndpoint.has_value()) {
        Message responseMsg =
          syncEndpoint->sendAwaitResponse(header, buffer, bufferSize);

        // Deserialise response
        if (!response->ParseFromArray(responseMsg.data().data(),
                                      responseMsg.data().size())) {
            throw std::runtime_error("Error deserialising message");
        }
    }
}
}
