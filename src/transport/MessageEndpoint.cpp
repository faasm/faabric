#include <faabric/transport/MessageEndpoint.h>

namespace faabric::transport {

MessageContext::MessageContext()
  : ctx(1)
{}

MessageContext::MessageContext(int overrideIoThreads)
  : ctx(overrideIoThreads)
{}

MessageContext::~MessageContext()
{
    this->close();
}

zmq::context_t& MessageContext::get()
{
    return this->ctx;
}

void MessageContext::close()
{
    this->ctx.close();
}

MessageEndpoint::MessageEndpoint(const std::string& hostIn, int portIn)
  : host(hostIn)
  , port(portIn)
{}

MessageEndpoint::~MessageEndpoint()
{
    this->close();
}

void MessageEndpoint::start(faabric::transport::MessageContext& context,
                            faabric::transport::ZeroMQSocketType sockTypeIn,
                            bool bind)
{
    auto logger = faabric::util::getLogger();
    std::string address = "tcp://" + host + ":" + std::to_string(port);
    // zmq::context_t ctx = I

    this->sockType = sockTypeIn;
    switch (this->sockType) {
        case faabric::transport::ZeroMQSocketType::PUSH:
            this->sock =
              std::make_unique<zmq::socket_t>(context.get(), zmq::socket_type::push);
            break;
        case faabric::transport::ZeroMQSocketType::PULL:
            this->sock =
              std::make_unique<zmq::socket_t>(context.get(), zmq::socket_type::pull);
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

void MessageEndpoint::sendMessage(char* serialisedMsg, size_t msgSize)
{
    // Pass a deallocation function for ZeroMQ to be zero-copy
    zmq::message_t msg(serialisedMsg, msgSize, [](void* data, void* hint) {
        delete[] (char*) data;
    });

    if (sockType != faabric::transport::ZeroMQSocketType::PUSH) {
        throw std::runtime_error("Can't send from a non-PUSH socket");
    }

    if (!this->sock->send(msg, zmq::send_flags::none)) {
        throw std::runtime_error("Error sending message over ZeroMQ");
    }
}

void MessageEndpoint::handleMessage()
{
    if (sockType != faabric::transport::ZeroMQSocketType::PULL) {
        throw std::runtime_error("Can't receive from a non-PULL socket");
    }

    zmq::message_t msg;
    if (!this->sock->recv(msg)) {
        throw std::runtime_error("Errror receiving message over ZeroMQ");
    }

    // Implementation specific message handling
    doHandleMessage(msg.data(), msg.size());
}

void MessageEndpoint::close()
{
    this->sock->close();
}
}
