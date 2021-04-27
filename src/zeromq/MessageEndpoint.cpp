#include <faabric/zeromq/MessageEndpoint.h>

namespace faabric::zeromq {
// static thread_local std::unique_ptr<zmq::socket_t> socket;

MessageEndpoint::MessageEndpoint(const std::string& hostIn, int portIn)
  : host(hostIn)
  , port(portIn)
  // , context(1)
{}

MessageEndpoint::~MessageEndpoint()
{
    this->sock->close();
}

void MessageEndpoint::start(zmq::context_t& context,
                            faabric::zeromq::ZeroMQSocketType sockTypeIn,
                            bool bind)
{
    auto logger = faabric::util::getLogger();
    std::string address = "tcp://" + host + ":" + std::to_string(port);

    this->sockType = sockTypeIn;
    switch (this->sockType) {
      case faabric::zeromq::ZeroMQSocketType::PUSH: 
        this->sock = std::make_unique<zmq::socket_t>(context,
                                                     zmq::socket_type::push);
        break;
      case faabric::zeromq::ZeroMQSocketType::PULL: 
        this->sock = std::make_unique<zmq::socket_t>(context,
                                                     zmq::socket_type::pull);
        break;
      default:
        throw std::runtime_error("Unrecognized ZeroMQ socket type");
    }

    // Note - only one socket may bind, but several can connect. This allows
    // for easy N - 1 or 1 - N PUSH/PULL patterns.
    if (bind) {
        this->sock->bind(address);
    } else {
        this->sock->connect(address);
    }
}

void MessageEndpoint::sendMessage(zmq::message_t& msg)
{
    if (!this->sock->send(msg, zmq::send_flags::none)) {
        throw std::runtime_error("Error sending message over ZeroMQ");
    }
}

void MessageEndpoint::handleMessage()
{
    if (sockType != faabric::zeromq::ZeroMQSocketType::PULL) {
        throw std::runtime_error("Can't receive from a non-PULL socket");
    }

    zmq::message_t msg;
    if (!this->sock->recv(msg)) {
        throw std::runtime_error("Errror receiving message over ZeroMQ");
    }

    // Implementation specific message handling
    doHandleMessage(msg);
}
}
