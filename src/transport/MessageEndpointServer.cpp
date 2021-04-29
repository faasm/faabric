#include <faabric/transport/MessageEndpointServer.h>
#include <faabric/util/logging.h>

// TODO - include this through a header
#define ZMQ_ETERM 156384765

namespace faabric::transport {
MessageEndpointServer::MessageEndpointServer(const std::string& host, int port)
  : MessageEndpoint(host, port)
{}

void MessageEndpointServer::start(faabric::transport::MessageContext& context)
{
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
                    break;
                }
                throw std::runtime_error("Errror in socket receiving message");
            }
        }
    });
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
}
