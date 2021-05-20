#include <faabric/transport/MessageEndpoint.h>

#include <faabric/util/gids.h>

/* Paste this on the file you want to debug. */
#include <stdio.h>
#include <execinfo.h>
static void print_trace(void) {
    char **strings;
    size_t i, size;
    enum Constexpr { MAX_SIZE = 1024 };
    void *array[MAX_SIZE];
    size = backtrace(array, MAX_SIZE);
    strings = backtrace_symbols(array, size);
    auto logger = faabric::util::getLogger();
    logger->warn("---------- BACKTRACE ----------");
    for (i = 0; i < size; i++) {
        logger->warn(strings[i]);
    }
    logger->warn("-------------------------------");
    free(strings);
}

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
        print_trace();
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
    assert(this->socket != nullptr);

    // Bind or connect the socket
    if (bind) {
        try {
            this->socket->bind(address);
        } catch (zmq::error_t& e) {
            throw std::runtime_error(
              fmt::format("Error binding socket to {}: {}", address, e.what()));
        }
    } else {
        try {
            this->socket->connect(address);
        } catch (zmq::error_t& e) {
            throw std::runtime_error(fmt::format(
              "Error connecting socket to {}: {}", address, e.what()));
        }
    }
}

void MessageEndpoint::send(uint8_t* serialisedMsg, size_t msgSize, bool more)
{
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
                close();
                // Re-throw to either notify error or unblock servers
                throw;
            } else {
                throw std::runtime_error(
                  fmt::format("Error receiving message: {}", e.what()));
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
            close();
            // Re-throw to either notify error or unblock servers
            throw;
        } else {
            throw std::runtime_error(
              fmt::format("Error receiving message: {}", e.what()));
        }
    }

    // Copy the received message to a buffer whose scope we control
    return Message(msg);
}

void MessageEndpoint::close()
{
    if (this->socket != nullptr) {

        // TODO -remove
        faabric::util::getLogger()->warn(fmt::format("Closing socket: {}", id));

        if (tid != std::this_thread::get_id()) {
            faabric::util::getLogger()->warn(
              "Closing socket from a different thread");
        }

        this->socket->close();
        this->socket = nullptr;
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
}
