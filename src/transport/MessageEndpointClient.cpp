#include <faabric/transport/MessageEndpointClient.h>
#include <faabric/util/logging.h>

namespace faabric::transport {
MessageEndpointClient::MessageEndpointClient(const std::string& host, int port)
  : SendMessageEndpoint(host, port)
{}

// Block until we receive a response from the server
Message MessageEndpointClient::awaitResponse(int port)
{
    // Wait for the response, open a temporary endpoint for it
    RecvMessageEndpoint endpoint(port);

    // Inherit timeouts on temporary endpoint
    endpoint.setRecvTimeoutMs(recvTimeoutMs);
    endpoint.setSendTimeoutMs(sendTimeoutMs);

    endpoint.open();

    Message receivedMessage;
    try {
        receivedMessage = endpoint.recv();
    } catch (MessageTimeoutException& ex) {
        endpoint.close();
        throw;
    }

    return receivedMessage;
}
}
