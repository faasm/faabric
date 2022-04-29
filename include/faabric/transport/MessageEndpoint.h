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
// messages, but also determine the period on which endpoints will re-poll, so
// they can be fairly long.
#define DEFAULT_SOCKET_TIMEOUT_MS 60000

// How long undelivered messages will hang around when the socket is closed,
// which also determines how long the context will hang for when closing if
// things haven't yet completed (usually only when there's an error).
#define LINGER_MS 100

// The header structure is:
// - Message code (uint8_t)
// - Message body size (size_t)
// - Message sequence number of in-order message delivery default -1 (int)
#define NO_HEADER 0
#define HEADER_MSG_SIZE (sizeof(uint8_t) + sizeof(size_t) + sizeof(int))

#define SHUTDOWN_HEADER 220
static const std::vector<uint8_t> shutdownPayload = { 0, 0, 1, 1 };

namespace faabric::transport {

enum MessageEndpointConnectType
{
    BIND = 0,
    CONNECT = 1,
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

    std::string getAddress();

  protected:
    const std::string address;
    const int timeoutMs;
    const std::thread::id tid;
    const int id;

    zmq::socket_t setUpSocket(zmq::socket_type socketType,
                              MessageEndpointConnectType connectType);

    void sendMessage(zmq::socket_t& socket,
                     uint8_t header,
                     const uint8_t* data,
                     size_t dataSize,
                     int sequenceNumber = -1);

    Message recvMessage(zmq::socket_t& socket, bool async);

  private:
    Message recvBuffer(zmq::socket_t& socket, size_t size);

    void sendBuffer(zmq::socket_t& socket,
                    const uint8_t* data,
                    size_t dataSize,
                    bool more);
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
              int sequenceNum = -1);

  protected:
    zmq::socket_t socket;
};

class AsyncInternalSendMessageEndpoint final : public MessageEndpoint
{
  public:
    AsyncInternalSendMessageEndpoint(const std::string& inProcLabel,
                                     int timeoutMs = DEFAULT_SOCKET_TIMEOUT_MS);

    void send(uint8_t header,
              const uint8_t* data,
              size_t dataSize,
              int sequenceNumber = -1);

  protected:
    zmq::socket_t socket;
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

  private:
    zmq::socket_t reqSocket;
};

class RecvMessageEndpoint : public MessageEndpoint
{
  public:
    /**
     * Constructor for external TCP sockets
     */
    RecvMessageEndpoint(int portIn, int timeoutMs, zmq::socket_type socketType);

    /**
     * Constructor for internal inproc sockets
     */
    RecvMessageEndpoint(std::string inProcLabel,
                        int timeoutMs,
                        zmq::socket_type socketType,
                        MessageEndpointConnectType connectType);

    virtual ~RecvMessageEndpoint(){};

    virtual Message recv();

    zmq::socket_t socket;

  protected:
    Message doRecv(bool async);
};

class FanInMessageEndpoint : public RecvMessageEndpoint
{
  public:
    FanInMessageEndpoint(int portIn,
                         int timeoutMs,
                         zmq::socket_type socketType);

    void attachFanOut(zmq::socket_t& fanOutSock);

    void stop();

  private:
    zmq::socket_t controlSock;
    std::string controlSockAddress;
};

class AsyncFanOutMessageEndpoint final : public MessageEndpoint
{
  public:
    AsyncFanOutMessageEndpoint(const std::string& inProcLabel,
                               int timeoutMs = DEFAULT_SOCKET_TIMEOUT_MS);

    zmq::socket_t socket;
};

class AsyncFanInMessageEndpoint final : public FanInMessageEndpoint
{
  public:
    AsyncFanInMessageEndpoint(int portIn,
                              int timeoutMs = DEFAULT_SOCKET_TIMEOUT_MS);
};

class SyncFanOutMessageEndpoint final : public RecvMessageEndpoint
{
  public:
    SyncFanOutMessageEndpoint(const std::string& inProcLabel,
                              int timeoutMs = DEFAULT_SOCKET_TIMEOUT_MS);
};

class SyncFanInMessageEndpoint final : public FanInMessageEndpoint
{
  public:
    SyncFanInMessageEndpoint(int portIn,
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

  protected:
    zmq::socket_t socket;
};

class MessageTimeoutException final : public faabric::util::FaabricException
{
  public:
    explicit MessageTimeoutException(std::string message)
      : FaabricException(std::move(message))
    {}
};
}
