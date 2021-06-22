#include <faabric/transport/MessageContext.h>
#include <faabric/transport/MessageEndpoint.h>
#include <faabric/util/gids.h>
#include <faabric/util/logging.h>

#include <unistd.h>

#define CATCH_ZMQ_ERR(op, label)                                               \
    try {                                                                      \
        op;                                                                    \
    } catch (zmq::error_t & e) {                                               \
        if (e.num() == ZMQ_ETERM) {                                            \
            SPDLOG_TRACE(                                                      \
              "Got ZeroMQ ETERM for {} on address {}", label, address);        \
        } else {                                                               \
            SPDLOG_ERROR("Caught ZeroMQ error for {} on address {}: {} ({})",  \
                         label,                                                \
                         address,                                              \
                         e.num(),                                              \
                         e.what());                                            \
            throw;                                                             \
        }                                                                      \
    }

namespace faabric::transport {

MessageEndpoint::MessageEndpoint(SocketType socketTypeIn,
                                 const std::string& hostIn,
                                 int portIn)
  : socketType(socketTypeIn)
  , host(hostIn)
  , port(portIn)
  , address("tcp://" + host + ":" + std::to_string(port))
  , tid(std::this_thread::get_id())
  , id(faabric::util::generateGid())
{}

MessageEndpoint::~MessageEndpoint()
{
    if (this->socket != nullptr) {
        SPDLOG_WARN("Destroying an open message endpoint!");
        this->close();
    }
}

void MessageEndpoint::open()
{
    // Check we are opening from the same thread. We assert not to incur in
    // costly checks when running a Release build.
    assert(tid == std::this_thread::get_id());

    // Note - only one socket may bind, but several can connect. This
    // allows for easy N - 1 or 1 - N PUSH/PULL patterns. Order between
    // bind and connect does not matter.
    std::shared_ptr<zmq::context_t> context = getGlobalMessageContext();
    switch (socketType) {
        case faabric::transport::SocketType::PUSH: {
            CATCH_ZMQ_ERR(
              {
                  this->socket = std::make_unique<zmq::socket_t>(
                    *context, zmq::socket_type::push);
              },
              "push_socket")

            CATCH_ZMQ_ERR(this->socket->connect(address), "connect")

            break;
        }
        case faabric::transport::SocketType::PULL: {
            CATCH_ZMQ_ERR(
              {
                  this->socket = std::make_unique<zmq::socket_t>(
                    *context, zmq::socket_type::pull);
              },
              "pull_socket")

            CATCH_ZMQ_ERR(this->socket->bind(address), "bind")

            break;
        }
        default: {
            throw std::runtime_error("Opening unrecognized socket type");
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

    zmq::send_flags sendFlags =
      more ? zmq::send_flags::sndmore : zmq::send_flags::none;

    CATCH_ZMQ_ERR(
      {
          auto res =
            this->socket->send(zmq::buffer(serialisedMsg, msgSize), sendFlags);
          if (res != msgSize) {
              SPDLOG_ERROR("Sent different bytes than expected (sent "
                           "{}, expected {})",
                           res.value_or(0),
                           msgSize);
              throw std::runtime_error("Error sending message");
          }
      },
      "send")
}

// By passing the expected recv buffer size, we instrument zeromq to receive on
// our provisioned buffer
Message MessageEndpoint::recv(int size)
{
    assert(tid == std::this_thread::get_id());
    assert(this->socket != nullptr);
    assert(size >= 0);

    if (size == 0) {
        return recvNoBuffer();
    }

    return recvBuffer(size);
}

Message MessageEndpoint::recvBuffer(int size)
{
    // Pre-allocate buffer to avoid copying data
    Message msg(size);

    CATCH_ZMQ_ERR(
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
              SPDLOG_TRACE("Endpoint received ETERM");
              return Message();
          }

          throw;
      },
      "recv_buffer")

    return msg;
}

Message MessageEndpoint::recvNoBuffer()
{
    // Allocate a message to receive data
    zmq::message_t msg;
    CATCH_ZMQ_ERR(
      try {
          auto res = this->socket->recv(msg);
          if (!res.has_value()) {
              SPDLOG_ERROR("Timed out receiving message with no size");
              throw MessageTimeoutException("Timed out receiving message");
          }
      } catch (zmq::error_t& e) {
          if (e.num() == ZMQ_ETERM) {
              SPDLOG_TRACE("Endpoint received ETERM");
              return Message();
          }
          throw;
      },
      "recv_no_buffer")

    // Copy the received message to a buffer whose scope we control
    return Message(msg);
}

void MessageEndpoint::close()
{
    if (this->socket != nullptr) {
        if (tid != std::this_thread::get_id()) {
            SPDLOG_WARN("Closing socket from a different thread");
        }

        // We duplicate the call to close() because when unbinding, we want to
        // block until we _actually_ have unbound, i.e. 0MQ has closed the
        // socket (which happens asynchronously). For connect()-ed sockets we
        // don't care.
        // Not blocking on un-bind can cause race-conditions when the underlying
        // system is slow at closing sockets, and the application relies a lot
        // on synchronous message-passing.
        switch (socketType) {
            case SocketType::PUSH: {
                CATCH_ZMQ_ERR(this->socket->disconnect(address), "disconnect")

                CATCH_ZMQ_ERR(this->socket->close(), "close_disconnect")
                break;
            }
            case SocketType::PULL: {
                // NOTE - unbinding a socket has a considerable overhead
                // compared to disconnecting it.
                CATCH_ZMQ_ERR(this->socket->unbind(address), "unbind")

                // TODO - could we reuse the monitor?
                zmq::monitor_t mon;
                const std::string monAddr =
                  "inproc://monitor_" + std::to_string(id);
                mon.init(*(this->socket), monAddr, ZMQ_EVENT_CLOSED);

                CATCH_ZMQ_ERR(this->socket->close(), "close_unbind")

                // Wait for this to complete
                mon.check_event(recvTimeoutMs);
                break;
            }
            default: {
                throw std::runtime_error("Closing unrecognised type of socket");
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
  : MessageEndpoint(SocketType::PUSH, hostIn, portIn)
{}

void SendMessageEndpoint::open()
{
    SPDLOG_TRACE(
      fmt::format("Opening socket: {} (SEND {}:{})", id, host, port));

    MessageEndpoint::open();
}

void SendMessageEndpoint::close()
{
    SPDLOG_TRACE(
      fmt::format("Closing socket: {} (SEND {}:{})", id, host, port));

    MessageEndpoint::close();
}

RecvMessageEndpoint::RecvMessageEndpoint(int portIn)
  : MessageEndpoint(SocketType::PULL, ANY_HOST, portIn)
{}

void RecvMessageEndpoint::open()
{
    SPDLOG_TRACE(
      fmt::format("Opening socket: {} (RECV {}:{})", id, host, port));

    MessageEndpoint::open();
}

void RecvMessageEndpoint::close()
{
    SPDLOG_TRACE(
      fmt::format("Closing socket: {} (RECV {}:{})", id, host, port));

    MessageEndpoint::close();
}
}
