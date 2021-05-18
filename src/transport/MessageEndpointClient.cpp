#include <faabric/transport/MessageEndpointClient.h>

namespace faabric::transport {
MessageEndpointClient::MessageEndpointClient(const std::string& host, int port)
  : MessageEndpoint(host, port)
{}

// Block until we receive a response from the server
Message MessageEndpointClient::awaitResponse(const std::string& host, int port)
{
    // Wait for the response, open a temporary endpoint for it
    // Note - we use a different host/port not to clash with existing server
    faabric::transport::MessageEndpoint endpoint(host, port);
    // Open the socket, must bind as server can't bind to a remote address
    endpoint.open(faabric::transport::getGlobalMessageContext(),
                  faabric::transport::SocketType::PULL,
                  true);
    Message receivedMessage = endpoint.recv();
    endpoint.close();

    return receivedMessage;
}
}
