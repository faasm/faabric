#pragma once

#include <faabric/transport/Message.h>
#include <faabric/util/exception.h>

#include <optional>
#include <thread>
#include <zmq.hpp>

// Defined in libzmq/include/zmq.h
#define ZMQ_ETERM ETERM

#define ANY_HOST "0.0.0.0"

// These timeouts should be long enough to permit sending and receiving large
// messages, but short enough not to hang around when something has gone wrong.
#define DEFAULT_RECV_TIMEOUT_MS 20000
#define DEFAULT_SEND_TIMEOUT_MS 20000

// How long undelivered messages will hang around when the socket is closed,
// which also determines how long the context will hang for when closing if
// things haven't yet completed (usually only when there's an error).
#define LINGER_MS 100

namespace faabric::transport {

// Note: sockets must be open-ed and close-ed from the _same_ thread. In a given
// communication group, one socket may bind, and all the rest must connect.
// Order does not matter.
class MessageEndpoint
{
  public:
    MessageEndpoint(const std::string& hostIn, int portIn, int timeoutMsIn);

    // Delete assignment and copy-constructor as we need to be very careful with
    // scoping and same-thread instantiation
    MessageEndpoint& operator=(const MessageEndpoint&) = delete;

    MessageEndpoint(const MessageEndpoint& ctx) = delete;

    std::string getHost();

    int getPort();

  protected:
    const std::string host;
    const int port;
    const std::string address;
    const int timeoutMs;
    const std::thread::id tid;
    const int id;

    zmq::socket_t setUpSocket(zmq::socket_type socketType, int socketPort);

    void doSend(zmq::socket_t& socket,
                const uint8_t* data,
                size_t dataSize,
                bool more);

    std::optional<Message> doRecv(zmq::socket_t& socket, int size = 0);

    std::optional<Message> recvBuffer(zmq::socket_t& socket, int size);

    std::optional<Message> recvNoBuffer(zmq::socket_t& socket);
};

class AsyncSendMessageEndpoint final : public MessageEndpoint
{
  public:
    AsyncSendMessageEndpoint(const std::string& hostIn,
                             int portIn,
                             int timeoutMs = DEFAULT_SEND_TIMEOUT_MS);

    void sendHeader(int header);

    void send(const uint8_t* data, size_t dataSize, bool more = false);

  private:
    zmq::socket_t pushSocket;
};

class SyncSendMessageEndpoint final : public MessageEndpoint
{
  public:
    SyncSendMessageEndpoint(const std::string& hostIn,
                            int portIn,
                            int timeoutMs = DEFAULT_SEND_TIMEOUT_MS);

    void sendHeader(int header);

    void sendRaw(const uint8_t* data, size_t dataSize);

    Message sendAwaitResponse(const uint8_t* data,
                              size_t dataSize,
                              bool more = false);

  private:
    zmq::socket_t reqSocket;
};

class RecvMessageEndpoint : public MessageEndpoint
{
  public:
    RecvMessageEndpoint(int portIn, int timeoutMs, zmq::socket_type socketType);

    virtual ~RecvMessageEndpoint(){};

    virtual std::optional<Message> recv(int size = 0);

  protected:
    zmq::socket_t socket;
};

class AsyncRecvMessageEndpoint final : public RecvMessageEndpoint
{
  public:
    AsyncRecvMessageEndpoint(int portIn,
                             int timeoutMs = DEFAULT_RECV_TIMEOUT_MS);

    std::optional<Message> recv(int size = 0) override;
};

class SyncRecvMessageEndpoint final : public RecvMessageEndpoint
{
  public:
    SyncRecvMessageEndpoint(int portIn,
                            int timeoutMs = DEFAULT_RECV_TIMEOUT_MS);

    std::optional<Message> recv(int size = 0) override;

    void sendResponse(const uint8_t* data, int size);
};

class MessageTimeoutException final : public faabric::util::FaabricException
{
  public:
    explicit MessageTimeoutException(std::string message)
      : FaabricException(std::move(message))
    {}
};
}
