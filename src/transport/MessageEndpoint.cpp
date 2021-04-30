#include <faabric/transport/MessageEndpoint.h>
#include <faabric/util/locks.h>

namespace faabric::transport {
MessageEndpoint::MessageEndpoint(const std::string& hostIn, int portIn)
  : host(hostIn)
  , port(portIn)
{}

MessageEndpoint::~MessageEndpoint()
{
    if (this->socket) {
        this->close();
    }
}

void MessageEndpoint::open(faabric::transport::MessageContext& context,
                           faabric::transport::SocketType sockType,
                           bool bind)
{
    std::string address =
      "tcp://" + this->host + ":" + std::to_string(this->port);

    // Note - only one socket may bind, but several can connect. This allows
    // for easy N - 1 or 1 - N PUSH/PULL patterns. Order between bind and
    // connect does not matter.
    switch (sockType) {
        case faabric::transport::SocketType::PUSH:
            this->socket = std::make_unique<zmq::socket_t>(
              context.get(), zmq::socket_type::push);
            break;
        case faabric::transport::SocketType::PULL:
            this->socket = std::make_unique<zmq::socket_t>(
              context.get(), zmq::socket_type::pull);
            break;
        default:
            throw std::runtime_error("Unrecognized socket type");
    }

    // Set linger period to zero. This avoids blocking if closing a socket with
    // outstanding messages.
    this->socket->set(zmq::sockopt::linger, 0);

    // Bind or connect the socket
    if (bind) {
        this->socket->bind(address);
    } else {
        this->socket->connect(address);
    }
}

void MessageEndpoint::send(char* serialisedMsg, size_t msgSize, bool more)
{
    if (!this->socket) {
        throw std::runtime_error("Trying to send from a null-pointing socket");
    }

    // Pass a deallocation function for ZeroMQ to be zero-copy
    zmq::message_t msg(serialisedMsg, msgSize, [](void* data, void* hint) {
        delete[](char*) data;
    });

    if (more) {
        if (!this->socket->send(msg, zmq::send_flags::sndmore)) {
            throw std::runtime_error("Error sending message through socket");
        }
    } else {
        if (!this->socket->send(msg, zmq::send_flags::none)) {
            throw std::runtime_error("Error sending message through socket");
        }
    }
}

void MessageEndpoint::recv()
{
    if (!this->socket) {
        throw std::runtime_error("Trying to recv from a null-pointing socket");
    }

    zmq::message_t msg;
    if (!this->socket->recv(msg)) {
        throw std::runtime_error("Error receiving message through socket");
    }

    // Implementation specific message handling
    doRecv(msg.data(), msg.size());
}

void MessageEndpoint::close()
{
    this->socket->close();
}
}
