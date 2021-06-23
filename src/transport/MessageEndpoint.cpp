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

MessageEndpoint::MessageEndpoint(zmq::socket_type socketTypeIn,
                                 const std::string& hostIn,
                                 int portIn,
                                 int timeoutMs)
  : socketType(socketTypeIn)
  , host(hostIn)
  , port(portIn)
  , address("tcp://" + host + ":" + std::to_string(port))
  , tid(std::this_thread::get_id())
  , id(faabric::util::generateGid())
{
    // Create the socket
    CATCH_ZMQ_ERR(socket =
                    zmq::socket_t(*getGlobalMessageContext(), socketType),
                  "socket_create")

    // Set socket options
    socket.set(zmq::sockopt::rcvtimeo, timeoutMs);
    socket.set(zmq::sockopt::sndtimeo, timeoutMs);

    // Note - only one socket may bind, but several can connect. This
    // allows for easy N - 1 or 1 - N PUSH/PULL patterns. Order between
    // bind and connect does not matter.
    switch (socketType) {
        case zmq::socket_type::push: {
            SPDLOG_TRACE("Opening push socket {}:{} (timeout {}ms)",
                         host,
                         port,
                         timeoutMs);
            CATCH_ZMQ_ERR(socket.connect(address), "connect")
            break;
        }
        case zmq::socket_type::pull: {
            SPDLOG_TRACE("Opening pull socket {}:{} (timeout {}ms)",
                         host,
                         port,
                         timeoutMs);
            CATCH_ZMQ_ERR(socket.bind(address), "bind")
            break;
        }
        default: {
            throw std::runtime_error("Opening unrecognized socket type");
        }
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

// ----------------------------------------------
// SEND ENDPOINT
// ----------------------------------------------

SendMessageEndpoint::SendMessageEndpoint(const std::string& hostIn,
                                         int portIn,
                                         int timeoutMs)
  : MessageEndpoint(zmq::socket_type::push, hostIn, portIn, timeoutMs)
{}

void SendMessageEndpoint::send(uint8_t* serialisedMsg,
                               size_t msgSize,
                               bool more)
{
    assert(tid == std::this_thread::get_id());

    if (this->socket == nullptr) {
        throw std::runtime_error("Sending on an unopened socket");
    }

    zmq::send_flags sendFlags =
      more ? zmq::send_flags::sndmore : zmq::send_flags::none;

    CATCH_ZMQ_ERR(
      {
          auto res =
            socket.send(zmq::buffer(serialisedMsg, msgSize), sendFlags);
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

// ----------------------------------------------
// RECV ENDPOINT
// ----------------------------------------------

RecvMessageEndpoint::RecvMessageEndpoint(int portIn)
  : MessageEndpoint(zmq::socket_type::pull,
                    ANY_HOST,
                    portIn,
                    DEFAULT_RECV_TIMEOUT_MS)
{}

Message RecvMessageEndpoint::recv(int size)
{
    assert(tid == std::this_thread::get_id());
    assert(size >= 0);

    if (this->socket == nullptr) {
        throw std::runtime_error("Receiving on an unopened socket");
    }

    if (size == 0) {
        return recvNoBuffer();
    }

    return recvBuffer(size);
}

Message RecvMessageEndpoint::recvBuffer(int size)
{
    // Pre-allocate buffer to avoid copying data
    Message msg(size);

    CATCH_ZMQ_ERR(
      try {
          auto res = socket.recv(zmq::buffer(msg.udata(), msg.size()));

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

Message RecvMessageEndpoint::recvNoBuffer()
{
    // Allocate a message to receive data
    zmq::message_t msg;
    CATCH_ZMQ_ERR(
      try {
          auto res = socket.recv(msg);
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
}
