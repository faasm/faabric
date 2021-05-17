#include <faabric/transport/MessageEndpoint.h>

namespace faabric::transport {
MessageEndpoint::MessageEndpoint(const std::string& hostIn, int portIn)
  : host(hostIn)
  , port(portIn)
  , tid(std::this_thread::get_id())
{}

MessageEndpoint::~MessageEndpoint()
{
    if (this->socket) {
        faabric::util::getLogger()->warn(
          "Destroying an open message endpoint!");
        this->close();
    }
}

void MessageEndpoint::open(faabric::transport::MessageContext& context,
                           faabric::transport::SocketType sockType,
                           bool bind)
{
    // Check we are opening from the same thread. We assert not to incur in
    // costly checks when running a Release build.
    assert(tid == std::this_thread::get_id());

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

    // Bind or connect the socket
    if (bind) {
        this->socket->bind(address);
    } else {
        this->socket->connect(address);
    }
}

void MessageEndpoint::send(uint8_t* serialisedMsg, size_t msgSize, bool more)
{
    assert(tid == std::this_thread::get_id());
    assert(this->socket != nullptr);

    // TODO - can we avoid this copy?
    zmq::message_t msg(serialisedMsg, msgSize);

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

// TODO - use optional size parameter
Message MessageEndpoint::recv(int size)
{
    assert(tid == std::this_thread::get_id());
    assert(this->socket != nullptr);
    assert(size >= 0);

    // Pre-allocate buffer to avoid copying data
    if (size > 0) {
        Message msg(size);

        if (!this->socket->recv(zmq::buffer(msg.udata(), msg.size()))) {
            throw std::runtime_error("Error receiving message through socket");
        }

        return msg;
    }

    // Allocate a message to receive data
    zmq::message_t msg;
    if (!this->socket->recv(msg)) {
        throw std::runtime_error("Error receiving message through socket");
    }

    // Copy the received message to a buffer whose scope we control
    return Message(msg);
}

void MessageEndpoint::close()
{
    this->socket->close();
    this->socket = nullptr;
}

std::string MessageEndpoint::getHost()
{
    return host;
}

int MessageEndpoint::getPort()
{
    return port;
}
}
