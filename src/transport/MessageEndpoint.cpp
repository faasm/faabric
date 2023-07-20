#include <faabric/transport/Message.h>
#include <faabric/transport/MessageEndpoint.h>
#include <faabric/transport/common.h>
#include <faabric/util/bytes.h>
#include <faabric/util/gids.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>

#include <array>
#include <chrono>
#include <concepts>
#include <optional>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <unistd.h>
#include <variant>
#include <vector>

#include <nng/nng.h>
#include <nng/protocol/pair1/pair.h>
#include <nng/protocol/pipeline0/pull.h>
#include <nng/protocol/pipeline0/push.h>
#include <nng/protocol/pubsub0/pub.h>
#include <nng/protocol/pubsub0/sub.h>
#include <nng/protocol/reqrep0/rep.h>
#include <nng/protocol/reqrep0/req.h>

#define RETRY_SLEEP_MS 1000

void checkNngError(int ec, std::string_view label, std::string_view address)
{
    if (ec != 0) {
        SPDLOG_ERROR("Caught NNG error for {} on address {}: {} ({})",
                     label,
                     address,
                     nng_strerror(ec),
                     ec);
        throw std::runtime_error(nng_strerror(ec));
    }
}

template<std::invocable Op>
void checkNngErrorRetryOnce(Op op,
                            std::string_view label,
                            std::string_view address)
{
    int ec = op();
    if (ec != 0) {
        SPDLOG_WARN("Caught NNG error for {} on address {}: {} ({}), retrying "
                    "once after {} ms",
                    label,
                    address,
                    nng_strerror(ec),
                    ec,
                    RETRY_SLEEP_MS);
        SLEEP_MS(RETRY_SLEEP_MS);
        ec = op();
        if (ec != 0) {
            SPDLOG_ERROR(
              "Caught NNG error on retry for {} on address {}: {} ({})",
              label,
              address,
              nng_strerror(ec),
              ec);
            throw std::runtime_error(nng_strerror(ec));
        }
    }
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

MessageEndpoint::~MessageEndpoint()
{
    if (socket.id != 0) {
        if (lingerMs > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(lingerMs));
        }
        close();
    }
}

std::string MessageEndpoint::getAddress()
{
    return address;
}

/**
 * This is where we set up all our sockets. It handles setting timeouts and
 * catching errors in the creation process, as well as logging and validating
 * our use of socket types and connection types.
 */
void MessageEndpoint::setUpSocket(SocketType socketType,
                                  MessageEndpointConnectType connectType)
{
    // Create the socket
    switch (connectType) {
        case (MessageEndpointConnectType::BIND): {
            SPDLOG_TRACE("Bind socket: {} {} (timeout {}ms)",
                         static_cast<int>(socketType),
                         address,
                         timeoutMs);
            switch (socketType) {
                case SocketType::pair: {
                    checkNngErrorRetryOnce(
                      [&]() { return nng_pair1_open(&socket); },
                      "bind",
                      address);
                    break;
                }
                case SocketType::pub: {
                    checkNngErrorRetryOnce(
                      [&]() { return nng_pub0_open(&socket); },
                      "bind",
                      address);
                    break;
                }
                case SocketType::pull: {
                    checkNngErrorRetryOnce(
                      [&]() { return nng_pull0_open(&socket); },
                      "bind",
                      address);
                    break;
                }
                case SocketType::push: {
                    checkNngErrorRetryOnce(
                      [&]() { return nng_push0_open(&socket); },
                      "bind",
                      address);
                    break;
                }
                case SocketType::rep: {
                    checkNngErrorRetryOnce(
                      [&]() { return nng_rep0_open(&socket); },
                      "bind",
                      address);
                    break;
                }
                default: {
                    SPDLOG_ERROR("Invalid bind socket type {} ({})",
                                 static_cast<int>(socketType),
                                 address);
                    throw std::runtime_error(
                      "Binding with invalid socket type");
                }
            }
            nng_listener listener;
            checkNngError(nng_listen(socket, address.c_str(), &listener, 0),
                          "nng_listen",
                          address);
            connectionManager = listener;
            break;
        }
        case (MessageEndpointConnectType::CONNECT): {
            SPDLOG_TRACE("Connect socket: {} {} (timeout {}ms)",
                         static_cast<int>(socketType),
                         address,
                         timeoutMs);
            switch (socketType) {
                case SocketType::pair: {
                    checkNngErrorRetryOnce(
                      [&]() { return nng_pair1_open(&socket); },
                      "connect",
                      address);
                    break;
                }
                case SocketType::pull: {
                    checkNngErrorRetryOnce(
                      [&]() { return nng_pull0_open(&socket); },
                      "connect",
                      address);
                    break;
                }
                case SocketType::push: {
                    checkNngErrorRetryOnce(
                      [&]() { return nng_push0_open(&socket); },
                      "connect",
                      address);
                    break;
                }
                case SocketType::rep: {
                    checkNngErrorRetryOnce(
                      [&]() { return nng_rep0_open(&socket); },
                      "connect",
                      address);
                    break;
                }
                case SocketType::req: {
                    checkNngErrorRetryOnce(
                      [&]() { return nng_req0_open(&socket); },
                      "connect",
                      address);
                    // Make sure to basically never resend messages
                    nng_socket_set_ms(
                      socket, NNG_OPT_REQ_RESENDTIME, 60'000'000);
                    break;
                }
                case SocketType::sub: {
                    checkNngErrorRetryOnce(
                      [&]() { return nng_sub0_open(&socket); },
                      "connect",
                      address);
                    break;
                }
                default: {
                    SPDLOG_ERROR("Invalid connect socket type {} ({})",
                                 static_cast<int>(socketType),
                                 address);
                    throw std::runtime_error(
                      "Connecting with unrecognized socket type");
                }
            }
            nng_dialer dialer;
            checkNngError(
              nng_dial(socket, address.c_str(), &dialer, NNG_FLAG_NONBLOCK),
              "nng_dial",
              address);
            connectionManager = dialer;
            break;
        }
        default: {
            SPDLOG_ERROR("Unrecognised socket connect type {}", connectType);
            throw std::runtime_error("Unrecognised connect type");
        }
    }

    nng_socket_set_ms(socket, NNG_OPT_RECVTIMEO, timeoutMs);
    nng_socket_set_ms(socket, NNG_OPT_SENDTIMEO, timeoutMs);
    if (!address.starts_with("inproc:")) {
        this->lingerMs = LINGER_MS;
    }
}

void MessageEndpoint::sendMessage(uint8_t header,
                                  const uint8_t* data,
                                  size_t dataSize,
                                  int sequenceNum,
                                  std::optional<nng_ctx> context)
{
    const size_t allocSize = HEADER_MSG_SIZE + dataSize;
    nng_msg* msg = nullptr;
    checkNngError(nng_msg_alloc(&msg, allocSize), "msg_alloc", address);
    uint8_t* buffer = reinterpret_cast<uint8_t*>(nng_msg_body(msg));
    std::uninitialized_fill_n(buffer, HEADER_MSG_SIZE, uint8_t(0));
    faabric::util::unalignedWrite<uint8_t>(header, buffer);
    faabric::util::unalignedWrite<uint64_t>(static_cast<uint64_t>(dataSize),
                                            buffer + sizeof(uint8_t));
    faabric::util::unalignedWrite<int32_t>(static_cast<int32_t>(sequenceNum),
                                           buffer + sizeof(uint8_t) +
                                             sizeof(uint64_t));
    std::copy_n(data, dataSize, buffer + HEADER_MSG_SIZE);

    nng_aio* aio = nullptr;
    if (int ec = nng_aio_alloc(&aio, nullptr, nullptr); ec < 0) {
        nng_msg_free(msg);
        checkNngError(ec, "nng_aio_alloc", address);
    }

    nng_aio_set_msg(aio, msg);

    if (context.has_value()) {
        nng_ctx_send(*context, aio);
    } else {
        nng_send_aio(socket, aio);
    }

    nng_aio_wait(aio);
    int ec = nng_aio_result(aio);
    nng_aio_free(aio);
    if (ec != 0) {
        SPDLOG_ERROR(
          "Error {} ({}) when sending messge to {} (seq: {} - ctx: {})",
          ec,
          nng_strerror(ec),
          address,
          sequenceNum,
          context.has_value());
        nng_msg_free(msg); // Owned by the socket if succeeded
        checkNngError(ec, "sendMessage", address);
    }
}

Message MessageEndpoint::recvMessage(bool async, std::optional<nng_ctx> context)
{
    nng_aio* aio = nullptr;
    checkNngError(
      nng_aio_alloc(&aio, nullptr, nullptr), "nng_aio_alloc", address);

    if (context.has_value()) {
        nng_ctx_recv(*context, aio);
    } else {
        nng_recv_aio(socket, aio);
    }

    nng_aio_wait(aio);
    const int ec = nng_aio_result(aio);
    nng_msg* rawMsg = nng_aio_get_msg(aio);
    nng_aio_free(aio);

    if (ec == NNG_ETIMEDOUT) {
        SPDLOG_TRACE(
          "Did not receive message within {}ms on {}", timeoutMs, address);
        return Message(MessageResponseCode::TIMEOUT);
    }
    if (ec == NNG_ECLOSED) {
        SPDLOG_TRACE("Endpoint {} received ECLOSED on recv", address);
        return Message(MessageResponseCode::TERM);
    }
    checkNngError(ec, "recvBuffer", address);

    // Takes ownership of the nng-allocated buffer and will free it
    Message msg(rawMsg);

    if (msg.allData().size() < HEADER_MSG_SIZE) {
        SPDLOG_ERROR(
          "Received a message that's too short, received: {} required: {}",
          msg.allData().size(),
          HEADER_MSG_SIZE);
        throw std::runtime_error("Message too small");
    }
    std::span<const uint8_t> dataBytes = msg.udata();

    if (dataBytes.size_bytes() != msg.getDeclaredDataSize()) {
        SPDLOG_ERROR("Received a different number of bytes {} than specified "
                     "in the message header {}",
                     dataBytes.size_bytes(),
                     msg.getDeclaredDataSize());
        throw std::runtime_error("Error receiving message");
    }

    SPDLOG_TRACE("Received message with header {} size {} on {}",
                 msg.getMessageCode(),
                 msg.udata().size(),
                 getAddress());

    if (msg.getMessageCode() == SHUTDOWN_HEADER) {
        if (std::equal(dataBytes.begin(),
                       dataBytes.end(),
                       shutdownPayload.cbegin(),
                       shutdownPayload.cend())) {
            SPDLOG_TRACE("Server thread on {} got shutdown message",
                         getAddress());

            // Send an empty response if in sync mode
            // (otherwise upstream socket will hang)
            if (!async) {
                std::array<uint8_t, 4> empty{ 0 };
                this->sendMessage(0, empty.data(), empty.size());
            }

            return Message(MessageResponseCode::TERM);
        }
    }

    return msg;
}

MessageContext MessageEndpoint::createContext()
{
    nng_ctx ctx = NNG_CTX_INITIALIZER;
    nng_ctx_open(&ctx, socket);
    return MessageContext(ctx);
}

void MessageEndpoint::close()
{
    // Switch depending on the type in the variant, closing the dialer or
    // listener depending on which one this socket was opened with.
    std::visit(
      [](auto&& value) {
          // see example at
          // https://en.cppreference.com/w/cpp/utility/variant/visit
          using T = std::decay_t<decltype(value)>;
          if constexpr (std::is_same_v<T, nng_dialer>) {
              nng_dialer_close(value);
          } else if constexpr (std::is_same_v<T, nng_listener>) {
              nng_listener_close(value);
          } else if constexpr (std::is_same_v<T, std::monostate>) {
              /* no-op */
          } else {
              // always false, T-dependent expression
              static_assert(std::is_same_v<T, T*>, "non-exhaustive visitor");
          }
      },
      connectionManager);
    connectionManager = std::monostate();

    int ec = nng_close(socket);
    if (ec != 0) {
        SPDLOG_WARN("Error when closing socket: {}", nng_strerror(ec));
    }
    socket = NNG_SOCKET_INITIALIZER;
}

// ----------------------------------------------
// ASYNC SEND ENDPOINT
// ----------------------------------------------

AsyncSendMessageEndpoint::AsyncSendMessageEndpoint(const std::string& hostIn,
                                                   int portIn,
                                                   int timeoutMs)
  : MessageEndpoint(hostIn, portIn, timeoutMs)
{
    setUpSocket(SocketType::push, MessageEndpointConnectType::CONNECT);
}

void AsyncSendMessageEndpoint::send(uint8_t header,
                                    const uint8_t* data,
                                    size_t dataSize,
                                    int sequenceNum)
{
    SPDLOG_TRACE("PUSH {} ({} bytes)", address, dataSize);
    sendMessage(header, data, dataSize, sequenceNum);
}

AsyncInternalSendMessageEndpoint::AsyncInternalSendMessageEndpoint(
  const std::string& inprocLabel,
  int timeoutMs)
  : MessageEndpoint("inproc://" + inprocLabel, timeoutMs)
{
    setUpSocket(SocketType::push, MessageEndpointConnectType::CONNECT);
}

void AsyncInternalSendMessageEndpoint::send(uint8_t header,
                                            const uint8_t* data,
                                            size_t dataSize,
                                            int sequenceNum)
{
    SPDLOG_TRACE("PUSH {} ({} bytes)", address, sequenceNum, dataSize);
    sendMessage(header, data, dataSize, sequenceNum);
}

// ----------------------------------------------
// SYNC SEND ENDPOINT
// ----------------------------------------------

SyncSendMessageEndpoint::SyncSendMessageEndpoint(const std::string& hostIn,
                                                 int portIn,
                                                 int timeoutMs)
  : MessageEndpoint(hostIn, portIn, timeoutMs)
{
    setUpSocket(SocketType::req, MessageEndpointConnectType::CONNECT);
}

void SyncSendMessageEndpoint::sendRaw(const uint8_t* data, size_t dataSize)
{
    SPDLOG_TRACE("REQ {} ({} bytes)", address, dataSize);
    sendMessage(NO_HEADER, data, dataSize);
}

Message SyncSendMessageEndpoint::sendAwaitResponse(uint8_t header,
                                                   const uint8_t* data,
                                                   size_t dataSize)
{
    SPDLOG_TRACE("REQ {} ({} bytes)", address, dataSize);
    auto ctx = createContext();
    sendMessage(header, data, dataSize, NO_SEQUENCE_NUM, ctx.context);

    // Do the receive
    SPDLOG_TRACE("RECV (REQ) {}", address);
    Message msg = recvMessage(false, ctx.context);
    if (msg.getResponseCode() != MessageResponseCode::SUCCESS) {
        SPDLOG_ERROR("Failed getting response on {}: code {}",
                     address,
                     static_cast<int>(msg.getResponseCode()));
        throw MessageTimeoutException("Error on waiting for response.");
    }

    return msg;
}

// ----------------------------------------------
// RECV ENDPOINT
// ----------------------------------------------

RecvMessageEndpoint::RecvMessageEndpoint(std::string inProcLabel,
                                         int timeoutMs,
                                         SocketType socketType,
                                         MessageEndpointConnectType connectType)
  : MessageEndpoint("inproc://" + inProcLabel, timeoutMs)
{
    setUpSocket(socketType, connectType);
}

RecvMessageEndpoint::RecvMessageEndpoint(int portIn,
                                         int timeoutMs,
                                         SocketType socketType)
  : MessageEndpoint(ANY_HOST, portIn, timeoutMs)
{
    setUpSocket(socketType, MessageEndpointConnectType::BIND);
}

Message RecvMessageEndpoint::recv()
{
    return doRecv(false);
}

Message RecvMessageEndpoint::doRecv(bool async)
{
    return recvMessage(async);
}

// ----------------------------------------------
// ASYNC FAN IN AND FAN OUT
// ----------------------------------------------

FanMessageEndpoint::FanMessageEndpoint(int portIn,
                                       int timeoutMs,
                                       SocketType socketType,
                                       bool isAsync)
  : MessageEndpoint(ANY_HOST, portIn, timeoutMs)
  , isAsync(isAsync)
{
    setUpSocket(socketType, MessageEndpointConnectType::BIND);
}

MessageContext FanMessageEndpoint::attachFanOut()
{
    return createContext();
}

void FanMessageEndpoint::stop()
{
    SPDLOG_TRACE("Terminating fan socket {}", getAddress());
    this->close();
}

Message FanMessageEndpoint::recv(const MessageContext& ctx)
{
    // Async (PULL) fan endpoints don't support context objects, a simple
    // receive is enough.
    return recvMessage(isAsync,
                       isAsync ? std::nullopt : std::optional(ctx.context));
}

void FanMessageEndpoint::sendResponse(const MessageContext& ctx,
                                      uint8_t header,
                                      const uint8_t* data,
                                      size_t dataSize)
{
    return sendMessage(header, data, dataSize, NO_SEQUENCE_NUM, ctx.context);
}

AsyncFanMessageEndpoint::AsyncFanMessageEndpoint(int portIn, int timeoutMs)
  : FanMessageEndpoint(portIn, timeoutMs, SocketType::pull, true)
{}

// ----------------------------------------------
// SYNC FAN IN AND FAN OUT
// ----------------------------------------------

SyncFanMessageEndpoint::SyncFanMessageEndpoint(int portIn, int timeoutMs)
  : FanMessageEndpoint(portIn, timeoutMs, SocketType::rep, false)
{}

// ----------------------------------------------
// ASYNC RECV ENDPOINT
// ----------------------------------------------

AsyncRecvMessageEndpoint::AsyncRecvMessageEndpoint(
  const std::string& inprocLabel,
  int timeoutMs)
  : RecvMessageEndpoint(inprocLabel,
                        timeoutMs,
                        SocketType::pull,
                        MessageEndpointConnectType::CONNECT)
{}

AsyncRecvMessageEndpoint::AsyncRecvMessageEndpoint(int portIn, int timeoutMs)
  : RecvMessageEndpoint(portIn, timeoutMs, SocketType::pull)
{}

Message AsyncRecvMessageEndpoint::recv()
{
    SPDLOG_TRACE("PULL {}", address);
    return RecvMessageEndpoint::recvMessage(true);
}

AsyncInternalRecvMessageEndpoint::AsyncInternalRecvMessageEndpoint(
  const std::string& inprocLabel,
  int timeoutMs)
  : RecvMessageEndpoint(inprocLabel,
                        timeoutMs,
                        SocketType::pull,
                        MessageEndpointConnectType::BIND)
{}

Message AsyncInternalRecvMessageEndpoint::recv()
{
    SPDLOG_TRACE("PULL {}", address);
    return RecvMessageEndpoint::recvMessage(true);
}

// ----------------------------------------------
// SYNC RECV ENDPOINT
// ----------------------------------------------

SyncRecvMessageEndpoint::SyncRecvMessageEndpoint(const std::string& inprocLabel,
                                                 int timeoutMs)
  : RecvMessageEndpoint(inprocLabel,
                        timeoutMs,
                        SocketType::rep,
                        MessageEndpointConnectType::CONNECT)
{}

SyncRecvMessageEndpoint::SyncRecvMessageEndpoint(int portIn, int timeoutMs)
  : RecvMessageEndpoint(portIn, timeoutMs, SocketType::rep)
{}

Message SyncRecvMessageEndpoint::recv()
{
    SPDLOG_TRACE("RECV (REP) {}", address);
    return doRecv(false);
}

void SyncRecvMessageEndpoint::sendResponse(uint8_t header,
                                           const uint8_t* data,
                                           size_t size)
{
    SPDLOG_TRACE("REP {} ({} bytes)", address, size);
    sendMessage(header, data, size);
}

// ----------------------------------------------
// INTERNAL DIRECT MESSAGE ENDPOINTS
// ----------------------------------------------

AsyncDirectRecvEndpoint::AsyncDirectRecvEndpoint(const std::string& inprocLabel,
                                                 int timeoutMs)
  : RecvMessageEndpoint(inprocLabel,
                        timeoutMs,
                        SocketType::pair,
                        MessageEndpointConnectType::BIND)
{}

Message AsyncDirectRecvEndpoint::recv()
{
    SPDLOG_TRACE("PAIR recv {}", address);
    return doRecv(true);
}

AsyncDirectSendEndpoint::AsyncDirectSendEndpoint(const std::string& inprocLabel,
                                                 int timeoutMs)
  : MessageEndpoint("inproc://" + inprocLabel, timeoutMs)
{
    setUpSocket(SocketType::pair, MessageEndpointConnectType::CONNECT);
}

void AsyncDirectSendEndpoint::send(uint8_t header,
                                   const uint8_t* data,
                                   size_t dataSize)
{
    SPDLOG_TRACE("PAIR send {} ({} bytes)", address, dataSize);
    sendMessage(header, data, dataSize);
}
}
