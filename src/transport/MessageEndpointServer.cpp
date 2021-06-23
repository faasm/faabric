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
        recvEndpoint = std::make_unique<RecvMessageEndpoint>(this->port);

        // Loop until we receive a shutdown message
        while (true) {
            try {
                bool messageReceived = this->recv();
                if (!messageReceived) {
                    SPDLOG_TRACE("Server received shutdown message");
                    break;
                }
            } catch (MessageTimeoutException& ex) {
                SPDLOG_TRACE("Server timed out with no messages, continuing");
                continue;
            }
        }
    });
}

void MessageEndpointServer::stop()
{
    // Send a shutdown message via a temporary endpoint
    SendMessageEndpoint e(recvEndpoint->getHost(), recvEndpoint->getPort());

    SPDLOG_TRACE("Sending shutdown message locally to {}:{}",
                 recvEndpoint->getHost(),
                 recvEndpoint->getPort());
    e.send(nullptr, 0);

    // Join the serving thread
    if (servingThread.joinable()) {
        servingThread.join();
    }
}

bool MessageEndpointServer::recv()
{
    // Receive header and body
    Message header = recvEndpoint->recv();

    // Detect shutdown condition
    if (header.size() == 0) {
        return false;
    }

    // Check the header was sent with ZMQ_SNDMORE flag
    if (!header.more()) {
        throw std::runtime_error("Header sent without SNDMORE flag");
    }

    // Check that there are no more messages to receive
    Message body = recvEndpoint->recv();
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
    SendMessageEndpoint sendEndpoint(returnHost,
                                     returnPort + REPLY_PORT_OFFSET);
    sendEndpoint.send(serialisedMsg, size);
}
}
