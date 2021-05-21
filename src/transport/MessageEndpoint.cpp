#include <faabric/transport/MessageEndpoint.h>

#include <faabric/util/gids.h>
#include <unistd.h>

namespace faabric::transport {
MessageEndpoint::MessageEndpoint(const std::string& hostIn, int portIn)
  : host(hostIn)
  , port(portIn)
  , tid(std::this_thread::get_id())
  , id(faabric::util::generateGid()) // REMOVE
{}

MessageEndpoint::~MessageEndpoint()
{
    if (this->socket != nullptr) {
        faabric::util::getLogger()->warn(
          "Destroying an open message endpoint!");
        // print_trace();
        // TODO -defaulting to false here?
        this->close(false);
    }
}

void MessageEndpoint::open(faabric::transport::MessageContext& context,
                           faabric::transport::SocketType sockType,
                           bool bind)
{
    try {
    // Check we are opening from the same thread. We assert not to incur in
    // costly checks when running a Release build.
    std::string address =
      "tcp://" + this->host + ":" + std::to_string(this->port);

    try {
    assert(tid == std::this_thread::get_id());

    // Note - only one socket may bind, but several can connect. This allows
    // for easy N - 1 or 1 - N PUSH/PULL patterns. Order between bind and
    // connect does not matter.
    switch (sockType) {
        case faabric::transport::SocketType::PUSH:
            try {
                this->socket = std::make_unique<zmq::socket_t>(
                  context.get(), zmq::socket_type::push);
            } catch (zmq::error_t& e) {
                throw std::runtime_error(
                  fmt::format("Error opening SEND socket to {}: {}",
                              address, e.what()));
            } 
            break;
        case faabric::transport::SocketType::PULL:
            try {
                this->socket = std::make_unique<zmq::socket_t>(
                  context.get(), zmq::socket_type::pull);
            } catch (zmq::error_t& e) {
                throw std::runtime_error(
                  fmt::format("Error opening RECV socket bound to {}: {}",
                              address, e.what()));
            }
            break;
        default:
            throw std::runtime_error("Unrecognized socket type");
    }
    assert(this->socket != nullptr);
    } catch (...) {
        faabric::util::getLogger()->error("Error in the first half");
        throw;
    }

    try {
    // Bind or connect the socket
    if (bind) {
        try {
            this->socket->bind(address);
        } catch (zmq::error_t& e) {
            faabric::util::getLogger()->error(
                "Error binding socket to {}: {}", address, e.what());
            throw;
            /*
            throw std::runtime_error(
              fmt::format("Error binding socket to {}: {}", address, e.what()));
            */
        } 
    } else {
        try {
            this->socket->connect(address);
        } catch (zmq::error_t& e) {
            faabric::util::getLogger()->error(
                "Error connecting socket to {}: {}", address, e.what());
            throw;
            /*
            throw std::runtime_error(fmt::format(
              "Error connecting socket to {}: {}", address, e.what()));
            */
        } 
    }
    } catch (...) {
        faabric::util::getLogger()->error("Error in the second half");
        throw;
    }
    } catch (...) {
        faabric::util::getLogger()->error("Error in MessageEndpoint::open");
        throw;
    }
}

void MessageEndpoint::send(uint8_t* serialisedMsg, size_t msgSize, bool more)
{
    try {
        assert(tid == std::this_thread::get_id());
        assert(this->socket != nullptr);

        if (more) {
            try {
                auto res = this->socket->send(zmq::buffer(serialisedMsg, msgSize),
                                              zmq::send_flags::sndmore);
                if (res != msgSize) {
                    throw std::runtime_error(fmt::format(
                      "Sent different bytes than expected (sent {}, expected {})",
                      res.value_or(0),
                      msgSize));
                }
            } catch (zmq::error_t& e) {
                throw std::runtime_error(
                  fmt::format("Error sending message: {}", e.what()));
            }
        } else {
            try {
                auto res = this->socket->send(zmq::buffer(serialisedMsg, msgSize),
                                              zmq::send_flags::none);
                if (res != msgSize) {
                    throw std::runtime_error(fmt::format(
                      "Sent different bytes than expected (sent {}, expected {})",
                      res.value_or(0),
                      msgSize));
                }
            } catch (zmq::error_t& e) {
                throw std::runtime_error(
                  fmt::format("Error sending message: {}", e.what()));
            }
        }
    } catch (...) {
        faabric::util::getLogger()->error("Error in MessageEndpoint::send");
        throw;
    }
}

// TODO - use optional size parameter
Message MessageEndpoint::recv(int size)
{
    try {
        assert(tid == std::this_thread::get_id());
        assert(this->socket != nullptr);
        assert(size >= 0);

        // Pre-allocate buffer to avoid copying data
        if (size > 0) {
            Message msg(size);

            try {
                auto res = this->socket->recv(zmq::buffer(msg.udata(), msg.size()));
                if (res.has_value() && (res->size != res->untruncated_size)) {
                    throw std::runtime_error(
                      fmt::format("Received more bytes than buffer can hold. "
                                  "Received: {}, capacity {}",
                                  res->untruncated_size,
                                  res->size));
                }
            } catch (zmq::error_t& e) {
                if (e.num() == ZMQ_ETERM) {
                    // Re-throw to either notify error or unblock servers
                    faabric::util::getLogger()->warn(
                      "Shutting endpoint down after receiving ETERM");
                return Message();
                } else {
                    faabric::util::getLogger()->error(
                      "Error receiving message: {}", e.what());
                    throw;
                }
            }

            return msg;
        }

        // Allocate a message to receive data
        zmq::message_t msg;
        try {
            auto res = this->socket->recv(msg);
            if (!res.has_value()) {
                throw std::runtime_error("Receive failed with EAGAIN");
            }
        } catch (zmq::error_t& e) {
            if (e.num() == ZMQ_ETERM) {
                // Return empty message to signify termination
                // TODO - change warn to trace
                faabric::util::getLogger()->warn(
                  "Shutting endpoint down after receiving ETERM");
                return Message();
            } else {
                faabric::util::getLogger()->error("Error receiving message: {}", e.what());
                throw;
            }
        } catch (...) {
            throw std::runtime_error(
              "Unrecognized exception receiving message through socket");
        }

        // Copy the received message to a buffer whose scope we control
        return Message(msg);
    } catch (...) {
        faabric::util::getLogger()->error("Error in MessageEndpoint::recv");
        throw;
    }
}

void MessageEndpoint::close(bool bind)
{
    try {
        if (this->socket != nullptr) {

            if (tid != std::this_thread::get_id()) {
                faabric::util::getLogger()->error(
                  "Closing socket from a different thread");
            }

            std::string address =
              "tcp://" + this->host + ":" + std::to_string(this->port);
            try {
                if (bind) {
                    this->socket->unbind(address);
                    usleep(1000 * 500);
                } else {
                    this->socket->disconnect(address);
                }
                this->socket->close();
            } catch (zmq::error_t& e) {
                if (e.num() != ZMQ_ETERM) {
                    faabric::util::getLogger()->error("Error closing socket: {}",
                                                      e.what());
                    throw;
                }
            }
            this->socket = nullptr;
        }
    } catch (...) {
        faabric::util::getLogger()->error("Error in MessageEndpoint::close");
        throw;
    }
}

std::string MessageEndpoint::getHost()
{
    return host;
}

int MessageEndpoint::getPort()
{
    return port;
}

/* Send and Recv Message Endpoints */

SendMessageEndpoint::SendMessageEndpoint(const std::string& hostIn, int portIn)
  : MessageEndpoint(hostIn, portIn)
{}

void SendMessageEndpoint::open(MessageContext& context)
{
    // TODO -remove
    faabric::util::getLogger()->warn(fmt::format("Opening socket: {} (SEND {}:{})",
                id, host, port));

    MessageEndpoint::open(context, SocketType::PUSH, false);
}

void SendMessageEndpoint::close()
{
    // TODO -remove
    faabric::util::getLogger()->warn(fmt::format("Closing socket: {} (SEND {}:{})",
                id, host, port));

    MessageEndpoint::close(false);
}

RecvMessageEndpoint::RecvMessageEndpoint(int portIn)
  : MessageEndpoint(ANY_HOST, portIn)
{}

void RecvMessageEndpoint::open(MessageContext& context)
{
    // TODO -remove
    faabric::util::getLogger()->warn(fmt::format("Opening socket: {} (RECV {}:{})",
                id, ANY_HOST, port));

    MessageEndpoint::open(context, SocketType::PULL, true);
}

void RecvMessageEndpoint::close()
{
    // TODO -remove
    faabric::util::getLogger()->warn(fmt::format("Closing socket: {} (RECV {}:{})",
                id, ANY_HOST, port));

    MessageEndpoint::close(true);
}
}
