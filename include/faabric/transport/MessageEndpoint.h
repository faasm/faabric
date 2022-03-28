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
// messages, note that they also determine the period on which endpoints will
// re-poll.
#define DEFAULT_RECV_TIMEOUT_MS 60000
#define DEFAULT_SEND_TIMEOUT_MS 60000

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

    void doSend(zmq::socket_t& socket,
                const uint8_t* data,
                size_t dataSize,
                bool more);

    Message doRecv(zmq::socket_t& socket, int size = 0);

    Message recvBuffer(zmq::socket_t& socket, int size);

    Message recvNoBuffer(zmq::socket_t& socket);
};

class AsyncSendMessageEndpoint final : public MessageEndpoint
{
  public:
    AsyncSendMessageEndpoint(const std::string& hostIn,
                             int portIn,
                             int timeoutMs = DEFAULT_SEND_TIMEOUT_MS);

    void sendHeader(int header);

    void send(const uint8_t* data, size_t dataSize, bool more = false);

    zmq::socket_t socket;
};

class AsyncInternalSendMessageEndpoint final : public MessageEndpoint
{
  public:
    AsyncInternalSendMessageEndpoint(const std::string& inProcLabel,
                                     int timeoutMs = DEFAULT_RECV_TIMEOUT_MS);

    void send(const uint8_t* data, size_t dataSize, bool more = false);

    zmq::socket_t socket;
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

    virtual Message recv(int size = 0);

    zmq::socket_t socket;
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
                               int timeoutMs = DEFAULT_RECV_TIMEOUT_MS);

    zmq::socket_t socket;
};

class AsyncFanInMessageEndpoint final : public FanInMessageEndpoint
{
  public:
    AsyncFanInMessageEndpoint(int portIn,
                              int timeoutMs = DEFAULT_RECV_TIMEOUT_MS);
};

class SyncFanOutMessageEndpoint final : public RecvMessageEndpoint
{
  public:
    SyncFanOutMessageEndpoint(const std::string& inProcLabel,
                              int timeoutMs = DEFAULT_RECV_TIMEOUT_MS);
};

class SyncFanInMessageEndpoint final : public FanInMessageEndpoint
{
  public:
    SyncFanInMessageEndpoint(int portIn,
                             int timeoutMs = DEFAULT_RECV_TIMEOUT_MS);
};

class AsyncRecvMessageEndpoint final : public RecvMessageEndpoint
{
  public:
    AsyncRecvMessageEndpoint(const std::string& inprocLabel,
                             int timeoutMs = DEFAULT_RECV_TIMEOUT_MS);

    AsyncRecvMessageEndpoint(int portIn,
                             int timeoutMs = DEFAULT_RECV_TIMEOUT_MS);

    Message recv(int size = 0) override;
};

class AsyncInternalRecvMessageEndpoint final : public RecvMessageEndpoint
{
  public:
    AsyncInternalRecvMessageEndpoint(const std::string& inprocLabel,
                                     int timeoutMs = DEFAULT_RECV_TIMEOUT_MS);

    Message recv(int size = 0) override;
};

class SyncRecvMessageEndpoint final : public RecvMessageEndpoint
{
  public:
    SyncRecvMessageEndpoint(const std::string& inprocLabel,
                            int timeoutMs = DEFAULT_RECV_TIMEOUT_MS);

    SyncRecvMessageEndpoint(int portIn,
                            int timeoutMs = DEFAULT_RECV_TIMEOUT_MS);

    Message recv(int size = 0) override;

    void sendResponse(const uint8_t* data, int size);
};

class AsyncDirectRecvEndpoint final : public RecvMessageEndpoint
{
  public:
    AsyncDirectRecvEndpoint(const std::string& inprocLabel,
                            int timeoutMs = DEFAULT_RECV_TIMEOUT_MS);

    Message recv(int size = 0) override;
};

class AsyncDirectSendEndpoint final : public MessageEndpoint
{
  public:
    AsyncDirectSendEndpoint(const std::string& inProcLabel,
                            int timeoutMs = DEFAULT_RECV_TIMEOUT_MS);

    void send(const uint8_t* data, size_t dataSize, bool more = false);

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
