#include <faabric/transport/MessageEndpoint.h>
#include <faabric/util/gids.h>
#include <faabric/util/logging.h>

#include <unistd.h>

namespace faabric::transport {
MessageEndpoint::MessageEndpoint(const std::string& hostIn, int portIn)
  : host(hostIn)
  , port(portIn)
  , address("tcp://" + host + ":" + std::to_string(port))
  , tid(std::this_thread::get_id())
  , id(faabric::util::generateGid())
{}

MessageEndpoint::~MessageEndpoint()
{
    if (this->socket != nullptr) {
        SPDLOG_WARN("Destroying an open message endpoint!");
        this->close(false);
    }
}

void MessageEndpoint::open(faabric::transport::MessageContext& context,
                           faabric::transport::SocketType sockType,
                           bool bind)
{
    // Check we are opening from the same thread. We assert not to incur in
    // costly checks when running a Release build.
    assert(tid == std::this_thread::get_id());

    // Note - only one socket may bind, but several can connect. This
    // allows for easy N - 1 or 1 - N PUSH/PULL patterns. Order between
    // bind and connect does not matter.
    switch (sockType) {
        case faabric::transport::SocketType::PUSH:
            try {
                this->socket = std::make_unique<zmq::socket_t>(
                  context.get(), zmq::socket_type::push);
            } catch (zmq::error_t& e) {
                SPDLOG_ERROR(
                  "Error opening SEND socket to {}: {}", address, e.what());
                throw;
            }
            break;
        case faabric::transport::SocketType::PULL:
            try {
                this->socket = std::make_unique<zmq::socket_t>(
                  context.get(), zmq::socket_type::pull);
            } catch (zmq::error_t& e) {
                SPDLOG_ERROR("Error opening RECV socket bound to {}: {}",
                             address,
                             e.what());
                throw;
            }

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
            SPDLOG_ERROR("Error binding socket to {}: {}", address, e.what());
            throw;
        }
    } else {
        try {
            this->socket->connect(address);
        } catch (zmq::error_t& e) {
            SPDLOG_ERROR(
              "Error connecting socket to {}: {}", address, e.what());
            throw;
        }
    }

    // Set socket options
    this->socket->setsockopt(ZMQ_RCVTIMEO, recvTimeoutMs);
    this->socket->setsockopt(ZMQ_SNDTIMEO, sendTimeoutMs);
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
                SPDLOG_ERROR("Sent different bytes than expected (sent "
                             "{}, expected {})",
                             res.value_or(0),
                             msgSize);
                throw std::runtime_error("Error sending message");
            }
        } catch (zmq::error_t& e) {
            SPDLOG_ERROR("Error sending message: {}", e.what());
            throw;
        }
    } else {
        try {
            auto res = this->socket->send(zmq::buffer(serialisedMsg, msgSize),
                                          zmq::send_flags::none);
            if (res != msgSize) {
                SPDLOG_ERROR("Sent different bytes than expected (sent "
                             "{}, expected {})",
                             res.value_or(0),
                             msgSize);
                throw std::runtime_error("Error sending message");
            }
        } catch (zmq::error_t& e) {
            SPDLOG_ERROR("Error sending message: {}", e.what());
            throw;
        }
    }
}

// By passing the expected recv buffer size, we instrument zeromq to receive on
// our provisioned buffer
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

            if (!res.has_value()) {
                SPDLOG_ERROR("Timed out receiving message of size {}", size);
                throw MessageTimeoutException("Timed out receiving message");
            }

            if (res.has_value() && (res->size != res->untruncated_size)) {
                SPDLOG_ERROR("Received more bytes than buffer can hold. "
                             "Received: {}, capacity {}",
                             res->untruncated_size,
                             res->size);
                throw std::runtime_error("Error receiving message");
            }
        } catch (zmq::error_t& e) {
            if (e.num() == ZMQ_ETERM) {
                // Return empty message to signify termination
                SPDLOG_TRACE("Shutting endpoint down after receiving ETERM");
                return Message();
            }

            // Print default message and rethrow
            SPDLOG_ERROR("Error receiving message: {} ({})", e.num(), e.what());
            throw;
        }

        return msg;
    }

    // Allocate a message to receive data
    zmq::message_t msg;
    try {
        auto res = this->socket->recv(msg);
        if (!res.has_value()) {
            SPDLOG_ERROR("Timed out receiving message with no size");
            throw MessageTimeoutException("Timed out receiving message");
        }
    } catch (zmq::error_t& e) {
        if (e.num() == ZMQ_ETERM) {
            // Return empty message to signify termination
            SPDLOG_TRACE("Shutting endpoint down after receiving ETERM");
            return Message();
        } else {
            SPDLOG_ERROR("Error receiving message: {} ({})", e.num(), e.what());
            throw;
        }
    }

    // Copy the received message to a buffer whose scope we control
    return Message(msg);
}

void MessageEndpoint::close(bool bind)
{
    if (this->socket != nullptr) {

        if (tid != std::this_thread::get_id()) {
            SPDLOG_WARN("Closing socket from a different thread");
        }

        // We duplicate the call to close() because when unbinding, we want to
        // block until we _actually_ have unbinded, i.e. 0MQ has closed the
        // socket (which happens asynchronously). For connect()-ed sockets we
        // don't care.
        // Not blobking on un-bind can cause race-conditions when the underlying
        // system is slow at closing sockets, and the application relies a lot
        // on synchronous message-passing.
        if (bind) {
            try {
                this->socket->unbind(address);
            } catch (zmq::error_t& e) {
                if (e.num() != ZMQ_ETERM) {
                    SPDLOG_ERROR("Error unbinding socket: {}", e.what());
                    throw;
                }
            }

            // NOTE - unbinding a socket has a considerable overhead compared to
            // disconnecting it.
            // TODO - could we reuse the monitor?
            try {
                zmq::monitor_t mon;
                const std::string monAddr =
                  "inproc://monitor_" + std::to_string(id);
                mon.init(*(this->socket), monAddr, ZMQ_EVENT_CLOSED);

                this->socket->close();
                mon.check_event(-1);
            } catch (zmq::error_t& e) {
                if (e.num() != ZMQ_ETERM) {
                    SPDLOG_ERROR("Error closing bind socket: {}", e.what());
                    throw;
                }
            }

        } else {
            try {
                this->socket->disconnect(address);
            } catch (zmq::error_t& e) {
                if (e.num() != ZMQ_ETERM) {
                    SPDLOG_ERROR("Error disconnecting socket: {}", e.what());
                    throw;
                }
            }
            try {
                this->socket->close();
            } catch (zmq::error_t& e) {
                SPDLOG_ERROR("Error closing connect socket: {}", e.what());
                throw;
            }
        }

        // Finally, null the socket
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

void MessageEndpoint::validateTimeout(int value)
{
    if (value <= 0) {
        SPDLOG_ERROR("Setting invalid timeout of {}", value);
        throw std::runtime_error("Setting invalid timeout");
    }

    if (socket != nullptr) {
        SPDLOG_ERROR("Setting timeout of {} after socket created", value);
        throw std::runtime_error("Setting timeout after socket created");
    }
}

void MessageEndpoint::setRecvTimeoutMs(int value)
{
    validateTimeout(value);
    recvTimeoutMs = value;
}

void MessageEndpoint::setSendTimeoutMs(int value)
{
    validateTimeout(value);
    sendTimeoutMs = value;
}

/* Send and Recv Message Endpoints */

SendMessageEndpoint::SendMessageEndpoint(const std::string& hostIn, int portIn)
  : MessageEndpoint(hostIn, portIn)
{}

void SendMessageEndpoint::open(MessageContext& context)
{
    SPDLOG_TRACE(
      fmt::format("Opening socket: {} (SEND {}:{})", id, host, port));

    MessageEndpoint::open(context, SocketType::PUSH, false);
}

void SendMessageEndpoint::close()
{
    SPDLOG_TRACE(
      fmt::format("Closing socket: {} (SEND {}:{})", id, host, port));

    MessageEndpoint::close(false);
}

RecvMessageEndpoint::RecvMessageEndpoint(int portIn)
  : MessageEndpoint(ANY_HOST, portIn)
{}

void RecvMessageEndpoint::open(MessageContext& context)
{
    SPDLOG_TRACE(
      fmt::format("Opening socket: {} (RECV {}:{})", id, host, port));

    MessageEndpoint::open(context, SocketType::PULL, true);
}

void RecvMessageEndpoint::close()
{
    SPDLOG_TRACE(
      fmt::format("Closing socket: {} (RECV {}:{})", id, host, port));

    MessageEndpoint::close(true);
}
}
