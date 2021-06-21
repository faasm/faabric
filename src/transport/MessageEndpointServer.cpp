#include <faabric/transport/MessageEndpointServer.h>
#include <faabric/util/logging.h>

#include <csignal>
#include <cstdlib>

namespace faabric::transport {
MessageEndpointServer::MessageEndpointServer(int portIn)
  : port(portIn)
{}

void MessageEndpointServer::start()
{
    // Start serving thread in background
    servingThread = std::thread([this] {
        endpoint = std::make_unique<RecvMessageEndpoint>(this->port);

        // Open message endpoint, and bind
        endpoint->open();

        // Loop until we receive a shutdown message
        while (true) {
            try {
                bool messageReceived = this->recv();
                if (!messageReceived) {
                    SPDLOG_TRACE("Server received shutdown message");
                    break;
                }
            } catch (MessageTimeoutException& ex) {
                continue;
            }
        }

        endpoint->close();
    });
}

void MessageEndpointServer::stop()
{
    // Send a shutdown message via a temporary endpoint
    SPDLOG_TRACE("Sending shutdown message locally to {}:{}",
                 endpoint->getHost(),
                 endpoint->getPort());
    SendMessageEndpoint e(endpoint->getHost(), endpoint->getPort());
    e.open();
    e.send(nullptr, 0);

    // Join the serving thread
    if (servingThread.joinable()) {
        servingThread.join();
    }

    e.close();
}

bool MessageEndpointServer::recv()
{
    // Check endpoint has been initialised
    assert(endpoint->socket != nullptr);

    // Receive header and body
    Message header = endpoint->recv();

    // Detect shutdown condition
    if (header.size() == 0) {
        return false;
    }

    // Check the header was sent with ZMQ_SNDMORE flag
    if (!header.more()) {
        throw std::runtime_error("Header sent without SNDMORE flag");
    }

    // Check that there are no more messages to receive
    Message body = endpoint->recv();
    if (body.more()) {
        throw std::runtime_error("Body sent with SNDMORE flag");
    }
    assert(body.udata() != nullptr);

    // Server-specific message handling
    doRecv(header, body);

    return true;
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
    endpoint.open();
    endpoint.send(serialisedMsg, size);
    endpoint.close();
}
}
