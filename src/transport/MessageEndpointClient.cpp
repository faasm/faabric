#include <faabric/transport/MessageEndpointClient.h>
#include <faabric/util/logging.h>

namespace faabric::transport {
MessageEndpointClient::MessageEndpointClient(const std::string& host,
                                             int port,
                                             int timeoutMs)
  : SendMessageEndpoint(host, port, timeoutMs)
{}

// Block until we receive a response from the server
Message MessageEndpointClient::awaitResponse(int port)
{
    // Wait for the response, open a temporary endpoint for it
    RecvMessageEndpoint endpoint(port);

    Message receivedMessage = endpoint.recv();

    return receivedMessage;
}
}
