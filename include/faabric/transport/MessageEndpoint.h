#pragma once

#include <faabric/transport/Message.h>
#include <faabric/util/exception.h>

#include <array>
#include <nng/nng.h>
#include <optional>
#include <string>
#include <thread>
#include <variant>
#include <vector>

#define ANY_HOST "0.0.0.0"

// These timeouts should be long enough to permit sending and receiving large
// messages, but also determine the period on which endpoints will re-poll, so
// they can be fairly long.
#define DEFAULT_SOCKET_TIMEOUT_MS 60000

// How long undelivered messages will hang around when the socket is closed,
// which also determines how long the context will hang for when closing if
// things haven't yet completed (usually only when there's an error).
#define LINGER_MS 100

namespace faabric::transport {

enum MessageEndpointConnectType
{
    BIND = 0,
    CONNECT = 1,
};

enum class SocketType
{
    pair,
    pub,
    sub,
    pull,
    push,
    rep,
    req
};

// Simple wrapper around nng_context handling its lifetime.
class MessageContext final
{
  public:
    MessageContext() = default;
    explicit MessageContext(nng_ctx context)
      : context(context)
    {}
    MessageContext(const MessageContext&) = delete;
    MessageContext& operator=(const MessageContext&) = delete;
    MessageContext(MessageContext&& rhs) { *this = std::move(rhs); }
    MessageContext& operator=(MessageContext&& rhs)
    {
        context = rhs.context;
        rhs.context = NNG_CTX_INITIALIZER;
        return *this;
    }

    ~MessageContext()
    {
        if (context.id != 0) {
            nng_ctx_close(context);
        }
    }

    nng_ctx context = NNG_CTX_INITIALIZER;
};

// Note: sockets must be open-ed and close-ed from the _same_ thread. In a given
// communication group, one socket may bind, and all the rest must connect.
// Order does not matter.
class MessageEndpoint
{
  public:
    MessageEndpoint(const std::string& hostIn, int portIn, int timeoutMsIn);

    MessageEndpoint(const std::string& addressIn, int timeoutMsIn);

    // Delete assignment and copy-constructor as we need to be very careful with
    // scoping and same-thread instantiation
    MessageEndpoint& operator=(const MessageEndpoint&) = delete;

    MessageEndpoint(const MessageEndpoint& ctx) = delete;

    virtual ~MessageEndpoint()
    {
        if (socket.id != 0) {
            close();
        }
    }

    std::string getAddress();

  protected:
    const std::string address;
    const int timeoutMs = -1;
    const std::thread::id tid;
    const int id = -1;

    nng_socket socket = NNG_SOCKET_INITIALIZER;
    std::variant<std::monostate, nng_dialer, nng_listener> connectionManager;

    void setUpSocket(SocketType socketType,
                     MessageEndpointConnectType connectType);

    void sendMessage(uint8_t header,
                     const uint8_t* data,
                     size_t dataSize,
                     int sequenceNumber = NO_SEQUENCE_NUM,
                     std::optional<nng_ctx> context = std::nullopt);

    Message recvMessage(bool async,
                        std::optional<nng_ctx> context = std::nullopt);

    MessageContext createContext();

    void close();
};

class AsyncSendMessageEndpoint final : public MessageEndpoint
{
  public:
    AsyncSendMessageEndpoint(const std::string& hostIn,
                             int portIn,
                             int timeoutMs = DEFAULT_SOCKET_TIMEOUT_MS);

    void send(uint8_t header,
              const uint8_t* data,
              size_t dataSize,
              int sequenceNum = NO_SEQUENCE_NUM);
};

class AsyncInternalSendMessageEndpoint final : public MessageEndpoint
{
  public:
    AsyncInternalSendMessageEndpoint(const std::string& inProcLabel,
                                     int timeoutMs = DEFAULT_SOCKET_TIMEOUT_MS);

    void send(uint8_t header,
              const uint8_t* data,
              size_t dataSize,
              int sequenceNumber = NO_SEQUENCE_NUM);
};

class SyncSendMessageEndpoint final : public MessageEndpoint
{
  public:
    SyncSendMessageEndpoint(const std::string& hostIn,
                            int portIn,
                            int timeoutMs = DEFAULT_SOCKET_TIMEOUT_MS);

    void sendRaw(const uint8_t* data, size_t dataSize);

    Message sendAwaitResponse(uint8_t header,
                              const uint8_t* data,
                              size_t dataSize);
};

class RecvMessageEndpoint : public MessageEndpoint
{
  public:
    /**
     * Constructor for external TCP sockets
     */
    RecvMessageEndpoint(int portIn, int timeoutMs, SocketType socketType);

    /**
     * Constructor for internal inproc sockets
     */
    RecvMessageEndpoint(std::string inProcLabel,
                        int timeoutMs,
                        SocketType socketType,
                        MessageEndpointConnectType connectType);

    virtual ~RecvMessageEndpoint(){};

    virtual Message recv();

  protected:
    Message doRecv(bool async);
};

class FanMessageEndpoint : public MessageEndpoint
{
  public:
    FanMessageEndpoint(int portIn,
                       int timeoutMs,
                       SocketType socketType,
                       bool isAsync);

    MessageContext attachFanOut();

    Message recv(const MessageContext& ctx);

    void sendResponse(const MessageContext& ctx,
                      uint8_t header,
                      const uint8_t* data,
                      size_t dataSize);

    void stop();

  private:
    std::string controlSockAddress;
    bool isAsync;
};

class AsyncFanMessageEndpoint final : public FanMessageEndpoint
{
  public:
    AsyncFanMessageEndpoint(int portIn,
                            int timeoutMs = DEFAULT_SOCKET_TIMEOUT_MS);
};

class SyncFanMessageEndpoint final : public FanMessageEndpoint
{
  public:
    SyncFanMessageEndpoint(int portIn,
                           int timeoutMs = DEFAULT_SOCKET_TIMEOUT_MS);
};

class AsyncRecvMessageEndpoint final : public RecvMessageEndpoint
{
  public:
    AsyncRecvMessageEndpoint(const std::string& inprocLabel,
                             int timeoutMs = DEFAULT_SOCKET_TIMEOUT_MS);

    AsyncRecvMessageEndpoint(int portIn,
                             int timeoutMs = DEFAULT_SOCKET_TIMEOUT_MS);

    Message recv() override;
};

class AsyncInternalRecvMessageEndpoint final : public RecvMessageEndpoint
{
  public:
    AsyncInternalRecvMessageEndpoint(const std::string& inprocLabel,
                                     int timeoutMs = DEFAULT_SOCKET_TIMEOUT_MS);

    Message recv() override;
};

class SyncRecvMessageEndpoint final : public RecvMessageEndpoint
{
  public:
    SyncRecvMessageEndpoint(const std::string& inprocLabel,
                            int timeoutMs = DEFAULT_SOCKET_TIMEOUT_MS);

    SyncRecvMessageEndpoint(int portIn,
                            int timeoutMs = DEFAULT_SOCKET_TIMEOUT_MS);

    Message recv() override;

    void sendResponse(uint8_t header, const uint8_t* data, size_t dataSize);
};

class AsyncDirectRecvEndpoint final : public RecvMessageEndpoint
{
  public:
    AsyncDirectRecvEndpoint(const std::string& inprocLabel,
                            int timeoutMs = DEFAULT_SOCKET_TIMEOUT_MS);

    Message recv() override;
};

class AsyncDirectSendEndpoint final : public MessageEndpoint
{
  public:
    AsyncDirectSendEndpoint(const std::string& inProcLabel,
                            int timeoutMs = DEFAULT_SOCKET_TIMEOUT_MS);

    void send(uint8_t header, const uint8_t* data, size_t dataSize);
};

class MessageTimeoutException final : public faabric::util::FaabricException
{
  public:
    explicit MessageTimeoutException(std::string message)
      : FaabricException(std::move(message))
    {}
};
}
