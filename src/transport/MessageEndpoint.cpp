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

    // TODO remove
    faabric::util::getLogger()->info("Open socket: {}", std::hash<std::thread::id>{}(tid));

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
    assert(tid == std::this_thread::get_id());
    assert(this->socket != nullptr);

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

// This method overload is used to send flatbuffers that deal with unsigned
// chars, and are stack-allocated.
void MessageEndpoint::send(uint8_t* serialisedMsg, size_t msgSize, bool more)
{
    assert(tid == std::this_thread::get_id());
    assert(this->socket != nullptr);

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

void MessageEndpoint::recv()
{
    assert(tid == std::this_thread::get_id());
    assert(this->socket != nullptr);

    zmq::message_t msg;
    if (!this->socket->recv(msg)) {
        throw std::runtime_error("Error receiving message through socket");
    }

    doRecv(msg.data(), msg.size());
}

void MessageEndpoint::close()
{
    // assert(tid == std::this_thread::get_id());

    // TODO remove
    faabric::util::getLogger()->info("Close socket: {}", std::hash<std::thread::id>{}(tid));

    this->socket->close();
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
