#include "faabric/transport/Message.h"
#include "faabric/util/bytes.h"
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

/**
 * This is where we set up all our sockets. It handles setting timeouts and
 * catching errors in the creation process, as well as logging and validating
 * our use of socket types and connection types.
 */
zmq::socket_t socketFactory(zmq::socket_type socketType,
                            MessageEndpointConnectType connectType,
                            int timeoutMs,
                            const std::string& address)
{
    zmq::socket_t socket;

    // Create the socket
    CATCH_ZMQ_ERR(socket =
                    zmq::socket_t(*getGlobalMessageContext(), socketType),
                  "socket_create")
    socket.set(zmq::sockopt::rcvtimeo, timeoutMs);
    socket.set(zmq::sockopt::sndtimeo, timeoutMs);

    // Setting linger here is essential to avoid infinite hangs
    socket.set(zmq::sockopt::linger, LINGER_MS);

    switch (connectType) {
        case (MessageEndpointConnectType::BIND): {
            switch (socketType) {
                case zmq::socket_type::dealer: {
                    SPDLOG_TRACE("Bind socket: dealer {} (timeout {}ms)",
                                 address,
                                 timeoutMs);
                    CATCH_ZMQ_ERR_RETRY_ONCE(socket.bind(address), "bind")
                    break;
                }
                case zmq::socket_type::pair: {
                    SPDLOG_TRACE("Bind socket: pair {} (timeout {}ms)",
                                 address,
                                 timeoutMs);
                    CATCH_ZMQ_ERR_RETRY_ONCE(socket.bind(address), "bind")
                    break;
                }
                case zmq::socket_type::pub: {
                    SPDLOG_TRACE(
                      "Bind socket: pub {} (timeout {}ms)", address, timeoutMs);
                    CATCH_ZMQ_ERR_RETRY_ONCE(socket.bind(address), "bind")
                    break;
                }
                case zmq::socket_type::pull: {
                    SPDLOG_TRACE("Bind socket: pull {} (timeout {}ms)",
                                 address,
                                 timeoutMs);
                    CATCH_ZMQ_ERR_RETRY_ONCE(socket.bind(address), "bind")
                    break;
                }
                case zmq::socket_type::push: {
                    SPDLOG_TRACE("Bind socket: push {} (timeout {}ms)",
                                 address,
                                 timeoutMs);
                    CATCH_ZMQ_ERR_RETRY_ONCE(socket.bind(address), "bind")
                    break;
                }
                case zmq::socket_type::rep: {
                    SPDLOG_TRACE(
                      "Bind socket: rep {} (timeout {}ms)", address, timeoutMs);
                    CATCH_ZMQ_ERR_RETRY_ONCE(socket.bind(address), "bind")
                    break;
                }
                case zmq::socket_type::router: {
                    SPDLOG_TRACE("Bind socket: router {} (timeout {}ms)",
                                 address,
                                 timeoutMs);
                    CATCH_ZMQ_ERR_RETRY_ONCE(socket.bind(address), "bind")
                    break;
                }
                default: {
                    SPDLOG_ERROR(
                      "Invalid bind socket type {} ({})", socketType, address);
                    throw std::runtime_error(
                      "Binding with invalid socket type");
                }
            }
            break;
        }
        case (MessageEndpointConnectType::CONNECT): {
            switch (socketType) {
                case zmq::socket_type::pair: {
                    SPDLOG_TRACE("Connect socket: pair {} (timeout {}ms)",
                                 address,
                                 timeoutMs);
                    CATCH_ZMQ_ERR_RETRY_ONCE(socket.connect(address), "connect")
                    break;
                }
                case zmq::socket_type::pull: {
                    SPDLOG_TRACE("Connect socket: pull {} (timeout {}ms)",
                                 address,
                                 timeoutMs);
                    CATCH_ZMQ_ERR_RETRY_ONCE(socket.connect(address), "connect")
                    break;
                }
                case zmq::socket_type::push: {
                    SPDLOG_TRACE("Connect socket: push {} (timeout {}ms)",
                                 address,
                                 timeoutMs);
                    CATCH_ZMQ_ERR_RETRY_ONCE(socket.connect(address), "connect")
                    break;
                }
                case zmq::socket_type::rep: {
                    SPDLOG_TRACE("Connect socket: rep {} (timeout {}ms)",
                                 address,
                                 timeoutMs);
                    CATCH_ZMQ_ERR_RETRY_ONCE(socket.connect(address), "connect")
                    break;
                }
                case zmq::socket_type::req: {
                    SPDLOG_TRACE("Connect socket: req {} (timeout {}ms)",
                                 address,
                                 timeoutMs);
                    CATCH_ZMQ_ERR_RETRY_ONCE(socket.connect(address), "connect")
                    break;
                }
                case zmq::socket_type::sub: {
                    SPDLOG_TRACE("Connect socket: sub {} (timeout {}ms)",
                                 address,
                                 timeoutMs);
                    CATCH_ZMQ_ERR_RETRY_ONCE(socket.connect(address), "connect")
                    break;
                }
                default: {
                    SPDLOG_ERROR("Invalid connect socket type {} ({})",
                                 socketType,
                                 address);
                    throw std::runtime_error(
                      "Connecting with unrecognized socket type");
                }
            }
            break;
        }
        default: {
            SPDLOG_ERROR("Unrecognised socket connect type {}", connectType);
            throw std::runtime_error("Unrecognised connect type");
        }
    }

    return socket;
}

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

std::string MessageEndpoint::getAddress()
{
    return address;
}

zmq::socket_t MessageEndpoint::setUpSocket(
  zmq::socket_type socketType,
  MessageEndpointConnectType connectType)
{
    return socketFactory(socketType, connectType, timeoutMs, address);
}

void MessageEndpoint::sendMessage(zmq::socket_t& socket,
                                  uint8_t header,
                                  const uint8_t* data,
                                  size_t dataSize)
{
    uint8_t buffer[HEADER_MSG_SIZE];
    faabric::util::unalignedWrite<uint8_t>(header, buffer);
    faabric::util::unalignedWrite<size_t>(dataSize, buffer + sizeof(uint8_t));

    sendBuffer(socket, buffer, HEADER_MSG_SIZE, true);
    sendBuffer(socket, data, dataSize, false);
}

Message MessageEndpoint::recvMessage(zmq::socket_t& socket, bool async)
{
    // Receive header and body
    Message headerMessage = recvBuffer(socket, HEADER_MSG_SIZE);
    if (headerMessage.getResponseCode() == MessageResponseCode::TIMEOUT) {
        SPDLOG_TRACE("Server on {}, looping after no message", getAddress());
        return Message(MessageResponseCode::TIMEOUT);
    }

    if (headerMessage.size() == shutdownHeader.size()) {
        if (headerMessage.dataCopy() == shutdownHeader) {
            SPDLOG_TRACE("Server thread on {} got shutdown message",
                         getAddress());

            // Send an empty response if in sync mode
            // (otherwise upstream socket will hang)
            if (!async) {
                std::vector<uint8_t> empty(4, 0);
                static_cast<SyncRecvMessageEndpoint*>(this)->sendResponse(
                  empty.data(), empty.size());
            }

            return Message(MessageResponseCode::TERM);
        }
    }

    assert(headerMessage.size() == HEADER_MSG_SIZE);
    uint8_t header =
      faabric::util::unalignedRead<uint8_t>(headerMessage.udata());
    size_t msgSize = faabric::util::unalignedRead<size_t>(
      headerMessage.udata() + sizeof(uint8_t));

    Message body = recvBuffer(socket, msgSize);
    body.setHeader(header);

    if (body.getResponseCode() != MessageResponseCode::SUCCESS) {
        SPDLOG_ERROR("Server on port {}, got header, error "
                     "on body: {}",
                     getAddress(),
                     body.getResponseCode());
        throw MessageTimeoutException("Server, got header, error on body");
    }

    return Message(std::move(body));
}

Message MessageEndpoint::recvBuffer(zmq::socket_t& socket, size_t size)
{
    // Pre-allocate message to avoid copying
    Message msg(size);

    CATCH_ZMQ_ERR(
      try {
          auto res = socket.recv(zmq::buffer(msg.data(), msg.size()));

          if (!res.has_value()) {
              SPDLOG_TRACE("Did not receive message size {} within {}ms on {}",
                           size,
                           timeoutMs,
                           address);
              return Message(MessageResponseCode::TIMEOUT);
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
              return Message(MessageResponseCode::TERM);
          }

          throw;
      },
      "recv_buffer")

    return Message(std::move(msg));
}

void MessageEndpoint::sendBuffer(zmq::socket_t& socket,
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

// ----------------------------------------------
// ASYNC SEND ENDPOINT
// ----------------------------------------------

AsyncSendMessageEndpoint::AsyncSendMessageEndpoint(const std::string& hostIn,
                                                   int portIn,
                                                   int timeoutMs)
  : MessageEndpoint(hostIn, portIn, timeoutMs)
{
    socket =
      setUpSocket(zmq::socket_type::push, MessageEndpointConnectType::CONNECT);
}

void AsyncSendMessageEndpoint::send(uint8_t header,
                                    const uint8_t* data,
                                    size_t dataSize)
{
    SPDLOG_TRACE("PUSH {} ({} bytes)", address, dataSize);
    sendMessage(socket, header, data, dataSize);
}

AsyncInternalSendMessageEndpoint::AsyncInternalSendMessageEndpoint(
  const std::string& inprocLabel,
  int timeoutMs)
  : MessageEndpoint("inproc://" + inprocLabel, timeoutMs)
{
    socket =
      setUpSocket(zmq::socket_type::push, MessageEndpointConnectType::CONNECT);
}

void AsyncInternalSendMessageEndpoint::send(uint8_t header,
                                            const uint8_t* data,
                                            size_t dataSize)
{
    SPDLOG_TRACE("PUSH {} ({} bytes)", address, dataSize);
    sendMessage(socket, header, data, dataSize);
}

// ----------------------------------------------
// SYNC SEND ENDPOINT
// ----------------------------------------------

SyncSendMessageEndpoint::SyncSendMessageEndpoint(const std::string& hostIn,
                                                 int portIn,
                                                 int timeoutMs)
  : MessageEndpoint(hostIn, portIn, timeoutMs)
{
    reqSocket =
      setUpSocket(zmq::socket_type::req, MessageEndpointConnectType::CONNECT);
}

void SyncSendMessageEndpoint::sendRaw(const uint8_t* data, size_t dataSize)
{
    SPDLOG_TRACE("REQ {} ({} bytes)", address, dataSize);
    sendMessage(reqSocket, 0, data, dataSize);
}

Message SyncSendMessageEndpoint::sendAwaitResponse(uint8_t header,
                                                   const uint8_t* data,
                                                   size_t dataSize)
{
    SPDLOG_TRACE("REQ {} ({} bytes)", address, dataSize);
    sendMessage(reqSocket, header, data, dataSize);

    // Do the receive
    SPDLOG_TRACE("RECV (REQ) {}", address);
    auto msg = recvMesage(reqSocket, false);
    if (msg.getResponseCode() != MessageResponseCode::SUCCESS) {
        SPDLOG_ERROR("Failed getting response on {}: code {}",
                     address,
                     msg.getResponseCode());
        throw MessageTimeoutException("Error on waiting for response.");
    }

    return Message(std::move(msg));
}

// ----------------------------------------------
// RECV ENDPOINT
// ----------------------------------------------

RecvMessageEndpoint::RecvMessageEndpoint(std::string inProcLabel,
                                         int timeoutMs,
                                         zmq::socket_type socketType,
                                         MessageEndpointConnectType connectType)
  : MessageEndpoint("inproc://" + inProcLabel, timeoutMs)
{
    socket = setUpSocket(socketType, connectType);
}

RecvMessageEndpoint::RecvMessageEndpoint(int portIn,
                                         int timeoutMs,
                                         zmq::socket_type socketType)
  : MessageEndpoint(ANY_HOST, portIn, timeoutMs)
{
    socket = setUpSocket(socketType, MessageEndpointConnectType::BIND);
}

Message RecvMessageEndpoint::recv(bool async)
{
    return recvMessage(socket, async);
}

// ----------------------------------------------
// ASYNC FAN IN AND FAN OUT
// ----------------------------------------------

FanInMessageEndpoint::FanInMessageEndpoint(int portIn,
                                           int timeoutMs,
                                           zmq::socket_type socketType)
  : RecvMessageEndpoint(portIn, timeoutMs, socketType)
  , controlSockAddress("inproc://" + std::to_string(portIn) + "-control")
{
    // Connect the control sock. Note that even though the control socket lives
    // longer than the control killer socket, we must *connect* here, not bind.
    controlSock = socketFactory(zmq::socket_type::sub,
                                MessageEndpointConnectType::CONNECT,
                                timeoutMs,
                                controlSockAddress);

    // Subscribe to all topics
    controlSock.set(zmq::sockopt::subscribe, zmq::str_buffer(""));
}

void FanInMessageEndpoint::attachFanOut(zmq::socket_t& fanOutSock)
{
    // Useful discussion on proxy_steerable here:
    // https://github.com/zeromq/cppzmq/issues/478
    SPDLOG_TRACE("Connecting proxy on {} ({})", address, controlSockAddress);
    zmq::proxy_steerable(socket, fanOutSock, zmq::socket_ref(), controlSock);
}

void FanInMessageEndpoint::stop()
{
    SPDLOG_TRACE("Sending TERMINATE on control socket {}", controlSockAddress);
    // Note that even though this killer socket is short-lived sending a message
    // to the control socket, we must *bind* here, not connect.
    zmq::socket_t controlKillerSock =
      socketFactory(zmq::socket_type::pub,
                    MessageEndpointConnectType::BIND,
                    timeoutMs,
                    controlSockAddress);

    controlKillerSock.send(zmq::str_buffer("TERMINATE"), zmq::send_flags::none);
    controlKillerSock.close();
}

AsyncFanOutMessageEndpoint::AsyncFanOutMessageEndpoint(
  const std::string& inprocLabel,
  int timeoutMs)
  : MessageEndpoint("inproc://" + inprocLabel, timeoutMs)
{
    socket =
      setUpSocket(zmq::socket_type::push, MessageEndpointConnectType::BIND);
}

AsyncFanInMessageEndpoint::AsyncFanInMessageEndpoint(int portIn, int timeoutMs)
  : FanInMessageEndpoint(portIn, timeoutMs, zmq::socket_type::pull)
{}

// ----------------------------------------------
// SYNC FAN IN AND FAN OUT
// ----------------------------------------------

SyncFanOutMessageEndpoint::SyncFanOutMessageEndpoint(
  const std::string& inProcLabel,
  int timeoutMs)
  : RecvMessageEndpoint(inProcLabel,
                        timeoutMs,
                        zmq::socket_type::dealer,
                        MessageEndpointConnectType::BIND)
{}

SyncFanInMessageEndpoint::SyncFanInMessageEndpoint(int portIn, int timeoutMs)
  : FanInMessageEndpoint(portIn, timeoutMs, zmq::socket_type::router)
{}

// ----------------------------------------------
// ASYNC RECV ENDPOINT
// ----------------------------------------------

AsyncRecvMessageEndpoint::AsyncRecvMessageEndpoint(
  const std::string& inprocLabel,
  int timeoutMs)
  : RecvMessageEndpoint(inprocLabel,
                        timeoutMs,
                        zmq::socket_type::pull,
                        MessageEndpointConnectType::CONNECT)
{}

AsyncRecvMessageEndpoint::AsyncRecvMessageEndpoint(int portIn, int timeoutMs)
  : RecvMessageEndpoint(portIn, timeoutMs, zmq::socket_type::pull)
{}

Message AsyncRecvMessageEndpoint::recv()
{
    SPDLOG_TRACE("PULL {}", address);
    return RecvMessageEndpoint::recvMessage(socket, true);
}

AsyncInternalRecvMessageEndpoint::AsyncInternalRecvMessageEndpoint(
  const std::string& inprocLabel,
  int timeoutMs)
  : RecvMessageEndpoint(inprocLabel,
                        timeoutMs,
                        zmq::socket_type::pull,
                        MessageEndpointConnectType::BIND)
{}

Message AsyncInternalRecvMessageEndpoint::recv()
{
    SPDLOG_TRACE("PULL {}", address);
    return RecvMessageEndpoint::recv();
}

// ----------------------------------------------
// SYNC RECV ENDPOINT
// ----------------------------------------------

SyncRecvMessageEndpoint::SyncRecvMessageEndpoint(const std::string& inprocLabel,
                                                 int timeoutMs)
  : RecvMessageEndpoint(inprocLabel,
                        timeoutMs,
                        zmq::socket_type::rep,
                        MessageEndpointConnectType::CONNECT)
{}

SyncRecvMessageEndpoint::SyncRecvMessageEndpoint(int portIn, int timeoutMs)
  : RecvMessageEndpoint(portIn, timeoutMs, zmq::socket_type::rep)
{}

Message SyncRecvMessageEndpoint::recv()
{
    SPDLOG_TRACE("RECV (REP) {}", address);
    return RecvMessageEndpoint::recv(false);
}

void SyncRecvMessageEndpoint::sendResponse(uint8_t header,
                                           const uint8_t* data,
                                           size_t size)
{
    SPDLOG_TRACE("REP {} ({} bytes)", address, size);
    sendMessage(socket, header, data, size);
}

// ----------------------------------------------
// INTERNAL DIRECT MESSAGE ENDPOINTS
// ----------------------------------------------

AsyncDirectRecvEndpoint::AsyncDirectRecvEndpoint(const std::string& inprocLabel,
                                                 int timeoutMs)
  : RecvMessageEndpoint(inprocLabel,
                        timeoutMs,
                        zmq::socket_type::pair,
                        MessageEndpointConnectType::BIND)
{}

Message AsyncDirectRecvEndpoint::recv()
{
    SPDLOG_TRACE("PAIR recv {}", address);
    return RecvMessageEndpoint::recv(true);
}

AsyncDirectSendEndpoint::AsyncDirectSendEndpoint(const std::string& inprocLabel,
                                                 int timeoutMs)
  : MessageEndpoint("inproc://" + inprocLabel, timeoutMs)
{
    socket =
      setUpSocket(zmq::socket_type::pair, MessageEndpointConnectType::CONNECT);
}

void AsyncDirectSendEndpoint::send(uint8_t header,
                                   const uint8_t* data,
                                   size_t dataSize)
{
    SPDLOG_TRACE("PAIR send {} ({} bytes)", address, dataSize);
    sendMessage(socket, header, data, dataSize);
}
}
