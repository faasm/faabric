#include <faabric/transport/MessageEndpointServer.h>

#include <csignal>
#include <cstdlib>

namespace faabric::transport {
MessageEndpointServer::MessageEndpointServer(int portIn)
  : port(portIn)
{}

void MessageEndpointServer::start()
{
    start(faabric::transport::getGlobalMessageContext());
}

void MessageEndpointServer::start(faabric::transport::MessageContext& context)
{
    // Start serving thread in background
    servingThread = std::thread([this, &context] {
        RecvMessageEndpoint serverEndpoint(this->port);

        // Open message endpoint, and bind
        serverEndpoint.open(context);
        assert(serverEndpoint.socket != nullptr);

        // Loop until context is terminated
        while (true) {
            int rc = this->recv(serverEndpoint);
            if (rc == ENDPOINT_SERVER_SHUTDOWN) {
                serverEndpoint.close();
                break;
            }
        }
    });
}

void MessageEndpointServer::stop()
{
    stop(faabric::transport::getGlobalMessageContext());
}

void MessageEndpointServer::stop(faabric::transport::MessageContext& context)
{
    // Join the serving thread
    if (servingThread.joinable()) {
        servingThread.join();
    }
}

int MessageEndpointServer::recv(RecvMessageEndpoint& endpoint)
{
    assert(endpoint.socket != nullptr);

    // Receive header and body
    Message header = endpoint.recv();

    // Detect shutdown condition
    if (header.udata() == nullptr) {
        return ENDPOINT_SERVER_SHUTDOWN;
    }

    // Check the header was sent with ZMQ_SNDMORE flag
    if (!header.more()) {
        throw std::runtime_error("Header sent without SNDMORE flag");
    }

    // Check that there are no more messages to receive
    Message body = endpoint.recv();
    if (body.more()) {
        throw std::runtime_error("Body sent with SNDMORE flag");
    }
    assert(body.udata() != nullptr);

    // Server-specific message handling
    doRecv(header, body);

    return 0;
}

// We create a new endpoint every time. Re-using them would be a possible
// optimisation if needed.
void MessageEndpointServer::sendResponse(uint8_t* serialisedMsg,
                                         int size,
                                         const std::string& returnHost,
                                         int returnPort)
{
    // Open the endpoint socket, server connects (not bind) to remote address
    SendMessageEndpoint endpoint(returnHost, returnPort + REPLY_PORT_OFFSET);
    endpoint.open(faabric::transport::getGlobalMessageContext());
    endpoint.send(serialisedMsg, size);
    endpoint.close();
}
}
