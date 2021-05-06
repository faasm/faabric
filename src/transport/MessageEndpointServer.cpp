#include <faabric/transport/MessageEndpointServer.h>

// Defined in libzmq/include/zmq.h (156384765)
#define ZMQ_ETERM ETERM

namespace faabric::transport {
MessageEndpointServer::MessageEndpointServer(const std::string& hostIn,
                                             int portIn)
  : host(hostIn)
  , port(portIn)
{}

void MessageEndpointServer::start()
{
    start(faabric::transport::getGlobalMessageContext());
}

void MessageEndpointServer::start(faabric::transport::MessageContext& context)
{
    // Start serving thread in background
    this->servingThread = std::thread([this, &context] {
        MessageEndpointClient serverEndpoint(this->host, this->port);

        // Open message endpoint, and bind
        serverEndpoint.open(
          context, faabric::transport::SocketType::PULL, true);

        // Loop until context is terminated
        while (true) {
            try {
                this->recv(serverEndpoint);
            } catch (zmq::error_t& e) {
                if (e.num() == ZMQ_ETERM) {
                    serverEndpoint.close();
                    break;
                }
                throw std::runtime_error(fmt::format(
                  "Errror in socket receiving message: {}", e.what()));
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
    // Note - different servers will concurrently close the server context, but
    // this structure is thread-safe, and the close operation idempotent.
    context.close();

    // Finally join the serving thread
    if (this->servingThread.joinable()) {
        this->servingThread.join();
    }
}

void MessageEndpointServer::recv(MessageEndpointClient& endpoint)
{
    // Receive header
    if (!endpoint.socket) {
        throw std::runtime_error("Trying to recv from a null-pointing socket");
    }

    zmq::message_t header;
    if (!endpoint.socket->recv(header)) {
        throw std::runtime_error("Error receiving message through socket");
    }

    // Check the header was sent with ZMQ_SNDMORE flag
    if (!header.more()) {
        throw std::runtime_error("Header sent without SNDMORE flag");
    }

    // Receive body
    zmq::message_t body;
    if (!endpoint.socket->recv(body)) {
        throw std::runtime_error("Error receiving message through socket");
    }

    // Check that there are no more messages to receive
    if (body.more()) {
        throw std::runtime_error("Body sent with SNDMORE flag");
    }

    // Server-specific message handling
    doRecv(header.data(), header.size(), body.data(), body.size());
}

// We create a new endpoint every time. Re-using them would be a possible
// optimisation if needed.
void MessageEndpointServer::sendResponse(char* serialisedMsg,
                                         int size,
                                         const std::string& returnHost,
                                         int returnPort)
{
    // Open the endpoint socket, server always binds
    faabric::transport::MessageEndpointClient endpoint(
      returnHost, returnPort + REPLY_PORT_OFFSET);
    endpoint.open(faabric::transport::getGlobalMessageContext(),
                  faabric::transport::SocketType::PUSH,
                  true);
    endpoint.send(serialisedMsg, size);
}
}
