#pragma once

#include <faabric/transport/Message.h>
#include <faabric/util/exception.h>

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

/*
 * Note, that sockets must be open-ed and close-ed from the _same_ thread. For a
 * proto://host:pair triple, one socket may bind, and all the rest must connect.
 * Order does not matter.
 */
class MessageEndpoint
{
  public:
    MessageEndpoint(const std::string& hostIn, int portIn, int timeoutMsIn);

    // Delete assignment and copy-constructor as we need to be very careful with
    // socping and same-thread instantiation
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

    Message doRecv(zmq::socket_t& socket, int size = 0);

    Message recvBuffer(zmq::socket_t& socket, int size);

    Message recvNoBuffer(zmq::socket_t& socket);
};

class AsyncSendMessageEndpoint : public MessageEndpoint
{
  public:
    AsyncSendMessageEndpoint(const std::string& hostIn,
                             int portIn,
                             int timeoutMs = DEFAULT_SEND_TIMEOUT_MS);

    void sendHeader(int header);

    void sendShutdown();

    void send(uint8_t* serialisedMsg, size_t msgSize, bool more = false);

  private:
    zmq::socket_t pushSocket;
};

class SyncSendMessageEndpoint : public MessageEndpoint
{
  public:
    SyncSendMessageEndpoint(const std::string& hostIn,
                            int portIn,
                            int timeoutMs = DEFAULT_SEND_TIMEOUT_MS);

    void sendHeader(int header);

    void sendShutdown();

    Message sendAwaitResponse(const uint8_t* serialisedMsg,
                              size_t msgSize,
                              bool more = false);

  private:
    zmq::socket_t reqSocket;
};

class AsyncRecvMessageEndpoint : public MessageEndpoint
{
  public:
    AsyncRecvMessageEndpoint(int portIn,
                             int timeoutMs = DEFAULT_RECV_TIMEOUT_MS);

    Message recv(int size = 0);

  private:
    zmq::socket_t pullSocket;
};

class SyncRecvMessageEndpoint : public MessageEndpoint
{
  public:
    SyncRecvMessageEndpoint(int portIn,
                            int timeoutMs = DEFAULT_RECV_TIMEOUT_MS);

    Message recv(int size = 0);

    void sendResponse(uint8_t* data, int size);

  private:
    zmq::socket_t repSocket;
};

class MessageTimeoutException : public faabric::util::FaabricException
{
  public:
    explicit MessageTimeoutException(std::string message)
      : FaabricException(std::move(message))
    {}
};
}
