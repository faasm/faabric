#include <faabric/transport/MessageEndpoint.h>
#include <faabric/transport/common.h>
#include <faabric/transport/context.h>
#include <faabric/util/gids.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>

#include <unistd.h>

#define RETRY_SLEEP_MS 1000

#define CATCH_ZMQ_ERR(op, label)                                               \
    try {                                                                      \
        op;                                                                    \
    } catch (zmq::error_t & e) {                                               \
        SPDLOG_ERROR("Caught ZeroMQ error for {} on address {}: {} ({})",      \
                     label,                                                    \
                     address,                                                  \
                     e.num(),                                                  \
                     e.what());                                                \
        throw;                                                                 \
    }

#define CATCH_ZMQ_ERR_RETRY_ONCE(op, label)                                    \
    try {                                                                      \
        op;                                                                    \
    } catch (zmq::error_t & e) {                                               \
        SPDLOG_WARN("Caught ZeroMQ error for {} on address {}: {} ({})",       \
                    label,                                                     \
                    address,                                                   \
                    e.num(),                                                   \
                    e.what());                                                 \
        SPDLOG_WARN("Retrying {} on address {}", label, address);              \
        SLEEP_MS(RETRY_SLEEP_MS);                                              \
        try {                                                                  \
            op;                                                                \
        } catch (zmq::error_t & e2) {                                          \
            SPDLOG_ERROR(                                                      \
              "Caught ZeroMQ error on retry for {} on address {}: {} ({})",    \
              label,                                                           \
              address,                                                         \
              e2.num(),                                                        \
              e2.what());                                                      \
            throw;                                                             \
        }                                                                      \
    }

namespace faabric::transport {

MessageEndpoint::MessageEndpoint(const std::string& addressIn, int timeoutMsIn)
  : address(addressIn)
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

// Convenience constructor for standard TCP ports
MessageEndpoint::MessageEndpoint(const std::string& hostIn,
                                 int portIn,
                                 int timeoutMsIn)
  : MessageEndpoint("tcp://" + hostIn + ":" + std::to_string(portIn),
                    timeoutMsIn)
{}

zmq::socket_t MessageEndpoint::setUpSocket(zmq::socket_type socketType)
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
              "New socket: req {} (timeout {}ms)", address, timeoutMs);
            CATCH_ZMQ_ERR_RETRY_ONCE(socket.connect(address), "connect")
            break;
        }
        case zmq::socket_type::push: {
            SPDLOG_TRACE(
              "New socket: push {} (timeout {}ms)", address, timeoutMs);
            CATCH_ZMQ_ERR_RETRY_ONCE(socket.connect(address), "connect")
            break;
        }
        case zmq::socket_type::pull: {

            SPDLOG_TRACE(
              "New socket: pull {} (timeout {}ms)", address, timeoutMs);
            if (forceConnectNotBind) {
                CATCH_ZMQ_ERR_RETRY_ONCE(socket.connect(address), "connect")
            } else {
                CATCH_ZMQ_ERR_RETRY_ONCE(socket.bind(address), "bind")
            }
            break;
        }
        case zmq::socket_type::rep: {
            SPDLOG_TRACE(
              "New socket: rep {} (timeout {}ms)", address, timeoutMs);
            if (forceConnectNotBind) {
                CATCH_ZMQ_ERR_RETRY_ONCE(socket.connect(address), "connect")
            } else {
                CATCH_ZMQ_ERR_RETRY_ONCE(socket.bind(address), "bind")
            }
            break;
        }
        case zmq::socket_type::router: {
            SPDLOG_TRACE(
              "New socket: router {} (timeout {}ms)", address, timeoutMs);
            CATCH_ZMQ_ERR_RETRY_ONCE(socket.bind(address), "bind")
            break;
        }
        case zmq::socket_type::dealer: {
            SPDLOG_TRACE(
              "New socket: dealer {} (timeout {}ms)", address, timeoutMs);
            CATCH_ZMQ_ERR_RETRY_ONCE(socket.bind(address), "bind")
            break;
        }
        default: {
            throw std::runtime_error("Opening unrecognized socket type");
        }
    }

    return socket;
}

void MessageEndpoint::doSend(zmq::socket_t& socket,
                             const uint8_t* data,
                             size_t dataSize,
                             bool more)
{
    assert(tid == std::this_thread::get_id());
    zmq::send_flags sendFlags =
      more ? zmq::send_flags::sndmore : zmq::send_flags::none;

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

std::optional<Message> MessageEndpoint::doRecv(zmq::socket_t& socket, int size)
{
    assert(tid == std::this_thread::get_id());
    assert(size >= 0);

    if (size == 0) {
        return recvNoBuffer(socket);
    }

    return recvBuffer(socket, size);
}

std::optional<Message> MessageEndpoint::recvBuffer(zmq::socket_t& socket,
                                                   int size)
{
    // Pre-allocate buffer to avoid copying data
    Message msg(size);

    CATCH_ZMQ_ERR(
      try {
          auto res = socket.recv(zmq::buffer(msg.udata(), msg.size()));

          if (!res.has_value()) {
              SPDLOG_TRACE("Timed out receiving message of size {}", size);
              return std::nullopt;
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
              SPDLOG_WARN("Endpoint {} received ETERM on recv", address);
              return Message();
          }

          throw;
      },
      "recv_buffer")

    return msg;
}

std::optional<Message> MessageEndpoint::recvNoBuffer(zmq::socket_t& socket)
{
    // Allocate a message to receive data
    zmq::message_t msg;
    CATCH_ZMQ_ERR(
      try {
          auto res = socket.recv(msg);
          if (!res.has_value()) {
              SPDLOG_TRACE("Timed out receiving message with no size");
              return std::nullopt;
          }
      } catch (zmq::error_t& e) {
          if (e.num() == ZMQ_ETERM) {
              SPDLOG_WARN("Endpoint {} received ETERM on recv", address);
              return Message();
          }
          throw;
      },
      "recv_no_buffer")

    // Copy the received message to a buffer whose scope we control
    return Message(msg);
}

// ----------------------------------------------
// ASYNC SEND ENDPOINT
// ----------------------------------------------

AsyncSendMessageEndpoint::AsyncSendMessageEndpoint(const std::string& hostIn,
                                                   int portIn,
                                                   int timeoutMs)
  : MessageEndpoint(hostIn, portIn, timeoutMs)
{
    pushSocket = setUpSocket(zmq::socket_type::push);
}

void AsyncSendMessageEndpoint::sendHeader(int header)
{
    uint8_t headerBytes = static_cast<uint8_t>(header);
    doSend(pushSocket, &headerBytes, sizeof(headerBytes), true);
}

void AsyncSendMessageEndpoint::send(const uint8_t* data,
                                    size_t dataSize,
                                    bool more)
{
    SPDLOG_TRACE("PUSH {} ({} bytes, more {})", address, dataSize, more);
    doSend(pushSocket, data, dataSize, more);
}

// ----------------------------------------------
// SYNC SEND ENDPOINT
// ----------------------------------------------

SyncSendMessageEndpoint::SyncSendMessageEndpoint(const std::string& hostIn,
                                                 int portIn,
                                                 int timeoutMs)
  : MessageEndpoint(hostIn, portIn, timeoutMs)
{
    reqSocket = setUpSocket(zmq::socket_type::req);
}

void SyncSendMessageEndpoint::sendHeader(int header)
{
    uint8_t headerBytes = static_cast<uint8_t>(header);
    doSend(reqSocket, &headerBytes, sizeof(headerBytes), true);
}

void SyncSendMessageEndpoint::sendRaw(const uint8_t* data, size_t dataSize)
{
    SPDLOG_TRACE("REQ {} ({} bytes)", address, dataSize);
    doSend(reqSocket, data, dataSize, false);
}

Message SyncSendMessageEndpoint::sendAwaitResponse(const uint8_t* data,
                                                   size_t dataSize,
                                                   bool more)
{
    SPDLOG_TRACE("REQ {} ({} bytes, more {})", address, dataSize, more);
    doSend(reqSocket, data, dataSize, more);

    // Do the receive
    SPDLOG_TRACE("RECV (REQ) {}", address);
    auto msgMaybe = recvNoBuffer(reqSocket);
    if (!msgMaybe.has_value()) {
        throw MessageTimeoutException("SendAwaitResponse timeout");
    }
    return msgMaybe.value();
}

// ----------------------------------------------
// RECV ENDPOINT
// ----------------------------------------------

RecvMessageEndpoint::RecvMessageEndpoint(std::string inProcLabel,
                                         int timeoutMs,
                                         zmq::socket_type socketType)
  : MessageEndpoint("inproc://" + inProcLabel, timeoutMs)
{
    // Because this is listening to a local dealer we have to force
    // a connect rather than a bind
    forceConnectNotBind = true;

    socket = setUpSocket(socketType);
}

RecvMessageEndpoint::RecvMessageEndpoint(int portIn,
                                         int timeoutMs,
                                         zmq::socket_type socketType)
  : MessageEndpoint(ANY_HOST, portIn, timeoutMs)
{
    socket = setUpSocket(socketType);
}

std::optional<Message> RecvMessageEndpoint::recv(int size)
{
    return doRecv(socket, size);
}

// ----------------------------------------------
// ROUTER AND DEALER ENDPOINTS
// ----------------------------------------------

DealerMessageEndpoint::DealerMessageEndpoint(const std::string& inProcLabel,
                                             int timeoutMs)
  : RecvMessageEndpoint(inProcLabel, timeoutMs, zmq::socket_type::dealer)
{}

RouterMessageEndpoint::RouterMessageEndpoint(int portIn, int timeoutMs)
  : RecvMessageEndpoint(portIn, timeoutMs, zmq::socket_type::router)
{}

void RouterMessageEndpoint::proxyWithDealer(
  std::unique_ptr<DealerMessageEndpoint>& dealer)
{
    // Connect this router to a dealer via a queue
    zmq::proxy(socket, dealer->socket);
}

// ----------------------------------------------
// ASYNC RECV ENDPOINT
// ----------------------------------------------

AsyncRecvMessageEndpoint::AsyncRecvMessageEndpoint(
  const std::string& inprocLabel,
  int timeoutMs)
  : RecvMessageEndpoint(inprocLabel, timeoutMs, zmq::socket_type::pull)
{}

AsyncRecvMessageEndpoint::AsyncRecvMessageEndpoint(int portIn, int timeoutMs)
  : RecvMessageEndpoint(portIn, timeoutMs, zmq::socket_type::pull)
{}

std::optional<Message> AsyncRecvMessageEndpoint::recv(int size)
{
    SPDLOG_TRACE("PULL {} ({} bytes)", address, size);
    return RecvMessageEndpoint::recv(size);
}

// ----------------------------------------------
// SYNC RECV ENDPOINT
// ----------------------------------------------

SyncRecvMessageEndpoint::SyncRecvMessageEndpoint(const std::string& inprocLabel,
                                                 int timeoutMs)
  : RecvMessageEndpoint(inprocLabel, timeoutMs, zmq::socket_type::rep)
{}

SyncRecvMessageEndpoint::SyncRecvMessageEndpoint(int portIn, int timeoutMs)
  : RecvMessageEndpoint(portIn, timeoutMs, zmq::socket_type::rep)
{}

std::optional<Message> SyncRecvMessageEndpoint::recv(int size)
{
    SPDLOG_TRACE("RECV (REP) {} ({} bytes)", address, size);
    return RecvMessageEndpoint::recv(size);
}

void SyncRecvMessageEndpoint::sendResponse(const uint8_t* data, int size)
{
    SPDLOG_TRACE("REP {} ({} bytes)", address, size);
    doSend(socket, data, size, false);
}
}
