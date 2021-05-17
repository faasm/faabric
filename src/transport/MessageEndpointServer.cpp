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
        MessageEndpoint serverEndpoint(this->host, this->port);

        // Open message endpoint, and bind
        serverEndpoint.open(
          context, faabric::transport::SocketType::PULL, true);
        assert(serverEndpoint.socket != nullptr);

        // Loop until context is terminated
        while (true) {
            try {
                this->recv(serverEndpoint);
            } catch (zmq::error_t& e) {
                if (e.num() == ZMQ_ETERM) {
                    serverEndpoint.close();
                    break;
                }
                throw std::runtime_error(
                  fmt::format("Errror in server socket loop (bound to {}:{}) "
                              "receiving message: {}",
                              serverEndpoint.getHost(),
                              serverEndpoint.getPort(),
                              e.what()));
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

void MessageEndpointServer::recv(MessageEndpoint& endpoint)
{
    assert(endpoint.socket != nullptr);

    // Receive header and body
    Message header = endpoint.recv();
    // Check the header was sent with ZMQ_SNDMORE flag
    if (!header.more()) {
        throw std::runtime_error("Header sent without SNDMORE flag");
    }
    Message body = endpoint.recv();
    // Check that there are no more messages to receive
    if (body.more()) {
        throw std::runtime_error("Body sent with SNDMORE flag");
    }

    // Server-specific message handling
    doRecv(header, body);
}

// We create a new endpoint every time. Re-using them would be a possible
// optimisation if needed.
void MessageEndpointServer::sendResponse(uint8_t* serialisedMsg,
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
    endpoint.close();
}
}
