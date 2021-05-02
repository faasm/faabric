#include <faabric/transport/MessageEndpointServer.h>
#include <faabric/util/logging.h>

// Defined in libzmq/include/zmq.h (156384765)
#define ZMQ_ETERM ETERM

namespace faabric::transport {
MessageEndpointServer::MessageEndpointServer(const std::string& host, int port)
  : MessageEndpoint(host, port)
{}

void MessageEndpointServer::start(faabric::transport::MessageContext& context)
{
    auto logger = faabric::util::getLogger();

    // Start serving thread in background
    this->servingThread = std::thread([this, &context] {
        // Open message endpoint, and bind
        this->open(context, faabric::transport::SocketType::PULL, true);

        // Loop until context is terminated
        while (true) {
            try {
                this->recv();
            } catch (zmq::error_t& e) {
                if (e.num() == ZMQ_ETERM) {
                    this->close();
                    break;
                }
                throw std::runtime_error("Errror in socket receiving message");
            }
        }
    });

    logger->debug("Stopping message endpoint server...");
}

void MessageEndpointServer::stop(faabric::transport::MessageContext& context)
{
    // Close the server context. This should make the serving thread exit the
    // infinite loop
    // Note - different servers will concurrently close the server context, but
    // this structure is thread-safe
    context.close();

    // Close the underlying message endpoint
    this->close();

    // Finally join the serving thread
    if (this->servingThread.joinable()) {
        this->servingThread.join();
    }
}

// Override the default behaviour knowing that we have to read both a header
// determining the function type, and the message body.
void MessageEndpointServer::recv()
{
    if (!this->socket) {
        throw std::runtime_error("Trying to recv from a null-pointing socket");
    }

    zmq::message_t header;
    if (!this->socket->recv(header)) {
        throw std::runtime_error("Error receiving message through socket");
    }

    // Check the header was sent with ZMQ_SNDMORE flag
    if (!header.more()) {
        throw std::runtime_error("Header sent without SNDMORE flag");
    }

    zmq::message_t body;
    if (!this->socket->recv(body)) {
        throw std::runtime_error("Error receiving message through socket");
    }

    // Implementation specific message handling
    doRecv(header.data(), header.size(), body.data(), body.size());
}
}
