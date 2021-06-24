#include <faabric/transport/MessageEndpoint.h>
#include <faabric/transport/common.h>
#include <faabric/transport/context.h>
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

MessageEndpoint::MessageEndpoint(const std::string& hostIn,
                                 int portIn,
                                 int timeoutMsIn)
  : host(hostIn)
  , port(portIn)
  , address("tcp://" + host + ":" + std::to_string(port))
  , timeoutMs(timeoutMsIn)
  , tid(std::this_thread::get_id())
  , id(faabric::util::generateGid())
{

    // Check and set socket timeout
    if (timeoutMs <= 0) {
        SPDLOG_ERROR("Setting invalid timeout of {}", timeoutMs);
        throw std::runtime_error("Setting invalid timeout");
    }
}

zmq::socket_t MessageEndpoint::setUpSocket(zmq::socket_type socketType,
                                           int socketPort)
{
    zmq::socket_t socket;

    // Create the socket
    CATCH_ZMQ_ERR(socket =
                    zmq::socket_t(*getGlobalMessageContext(), socketType),
                  "socket_create")
    socket.set(zmq::sockopt::rcvtimeo, timeoutMs);
    socket.set(zmq::sockopt::sndtimeo, timeoutMs);

    // Note - setting linger here is essential to avoid infinite hangs
    socket.set(zmq::sockopt::linger, LINGER_MS);

    switch (socketType) {
        case zmq::socket_type::req: {
            SPDLOG_TRACE(
              "Opening req socket {}:{} (timeout {}ms)", host, port, timeoutMs);
            CATCH_ZMQ_ERR(socket.connect(address), "connect")
            break;
        }
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
        case zmq::socket_type::rep: {
            SPDLOG_TRACE(
              "Opening rep socket {}:{} (timeout {}ms)", host, port, timeoutMs);
            CATCH_ZMQ_ERR(socket.bind(address), "bind")
            break;
        }
        default: {
            throw std::runtime_error("Opening unrecognized socket type");
        }
    }
}

void MessageEndpoint::doSend(zmq::socket_t& socket,
                             uint8_t* data,
                             size_t dataSize,
                             bool more)
{
    assert(tid == std::this_thread::get_id());
    zmq::send_flags sendFlags =
      more ? zmq::send_flags::sndmore : zmq::send_flags::dontwait;

    CATCH_ZMQ_ERR(
      {
          auto res = socket.send(zmq::buffer(data, dataSize), sendFlags);
          if (res != dataSize) {
              SPDLOG_ERROR("Sent different bytes than expected (sent "
                           "{}, expected {})",
                           res.value_or(0),
                           dataSize);
              throw std::runtime_error("Error sending message");
          }
      },
      "send")
}

Message MessageEndpoint::doRecv(zmq::socket_t& socket, int size)
{
    assert(tid == std::this_thread::get_id());
    assert(size >= 0);

    if (size == 0) {
        return recvNoBuffer(socket);
    }

    return recvBuffer(socket, size);
}

Message MessageEndpoint::recvBuffer(zmq::socket_t& socket, int size)
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

Message MessageEndpoint::recvNoBuffer(zmq::socket_t& socket)
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

std::string MessageEndpoint::getHost()
{
    return host;
}

int MessageEndpoint::getPort()
{
    return port;
}

// ----------------------------------------------
// ASYNC SEND ENDPOINT
// ----------------------------------------------

AsyncSendMessageEndpoint::AsyncSendMessageEndpoint(const std::string& hostIn,
                                                   int portIn,
                                                   int timeoutMs)
  : MessageEndpoint(hostIn, portIn, timeoutMs)
{
    pushSocket = setUpSocket(zmq::socket_type::push, portIn);
}

void AsyncSendMessageEndpoint::send(uint8_t* serialisedMsg,
                                    size_t msgSize,
                                    bool more)
{
    doSend(pushSocket, serialisedMsg, msgSize, more);
}

// ----------------------------------------------
// SYNC SEND ENDPOINT
// ----------------------------------------------

SyncSendMessageEndpoint::SyncSendMessageEndpoint(const std::string& hostIn,
                                                 int portIn,
                                                 int timeoutMs)
  : MessageEndpoint(hostIn, portIn, timeoutMs)
{
    reqSocket = setUpSocket(zmq::socket_type::req, portIn + 1);
}

void SyncSendMessageEndpoint::sendHeader(int header)
{
    uint8_t headerBytes = static_cast<uint8_t>(header);
    doSend(reqSocket, &headerBytes, sizeof(headerBytes), true);
}

Message SyncSendMessageEndpoint::sendAwaitResponse(uint8_t* serialisedMsg,
                                                   size_t msgSize,
                                                   bool more)
{
    doSend(reqSocket, serialisedMsg, msgSize, more);

    // Do the receive
    zmq::message_t msg;
    CATCH_ZMQ_ERR(
      try {
          auto res = reqSocket.recv(msg);
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
      "send_recv")

    return Message(msg);
}

// ----------------------------------------------
// ASYNC RECV ENDPOINT
// ----------------------------------------------

AsyncRecvMessageEndpoint::AsyncRecvMessageEndpoint(int portIn, int timeoutMs)
  : MessageEndpoint(ANY_HOST, portIn, timeoutMs)
{
    pullSocket = setUpSocket(zmq::socket_type::pull, portIn);
}

Message AsyncRecvMessageEndpoint::recv(int size)
{
    return doRecv(pullSocket, size);
}

// ----------------------------------------------
// SYNC RECV ENDPOINT
// ----------------------------------------------
//
SyncRecvMessageEndpoint::SyncRecvMessageEndpoint(int portIn, int timeoutMs)
  : MessageEndpoint(ANY_HOST, portIn, timeoutMs)
{
    repSocket = setUpSocket(zmq::socket_type::rep, portIn + 1);
}

Message SyncRecvMessageEndpoint::recv(int size)
{
    return doRecv(repSocket, size);
}

// We create a new endpoint every time. Re-using them would be a possible
// optimisation if needed.
void SyncRecvMessageEndpoint::sendResponse(uint8_t* data, int size)
{
    doSend(repSocket, data, size, false);
}
}
