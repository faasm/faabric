#include <faabric/transport/MessageEndpointClient.h>

namespace faabric::transport {
MessageEndpointClient::MessageEndpointClient(const std::string& host, int port)
  : MessageEndpoint(host, port)
{}

MessageEndpointClient::~MessageEndpointClient()
{
    this->close();
}

void MessageEndpointClient::recv(char*& msgData, int& size)
{
    MessageEndpoint::recv();
    msgData = this->msgData;
    size = this->msgSize;
}

void MessageEndpointClient::doRecv(void* msgData, int size)
{
    this->msgData = new char[size]();
    memcpy(this->msgData, (char*)msgData, size);
    this->msgSize = size;
}

// Block until we receive a response from the server
void MessageEndpointClient::awaitResponse(const std::string& host,
                                          int port,
                                          char*& data,
                                          int& size)
{
    // Wait for the response, open a temporary endpoint for it
    // Note - we use a different host/port not to clash with existing server
    faabric::transport::MessageEndpointClient endpoint(host, port);
    // Open the socket, client does not bind
    endpoint.open(faabric::transport::getGlobalMessageContext(),
                  faabric::transport::SocketType::PULL,
                  false);
    endpoint.recv(data, size);
    endpoint.close();
}
}
