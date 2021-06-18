#pragma once

#include <google/protobuf/message.h>

#include <faabric/transport/Message.h>
#include <faabric/transport/MessageContext.h>
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

namespace faabric::transport {
enum class SocketType
{
    PUSH,
    PULL
};

/* Wrapper arround zmq::socket_t
 *
 * Thread-unsafe socket-like object. MUST be open-ed and close-ed from the
 * _same_ thread. For a proto://host:pair triple, one socket may bind, and all
 * the rest must connect. Order does not matter. Sockets either send (PUSH)
 * or recv (PULL) data.
 */
class MessageEndpoint
{
  public:
    MessageEndpoint(const std::string& hostIn, int portIn);

    // Message endpoints shouldn't be assigned as ZeroMQ sockets are not thread
    // safe
    MessageEndpoint& operator=(const MessageEndpoint&) = delete;

    // Neither copied
    MessageEndpoint(const MessageEndpoint& ctx) = delete;

    ~MessageEndpoint();

    void open(faabric::transport::MessageContext& context,
              faabric::transport::SocketType sockTypeIn,
              bool bind);

    void close(bool bind);

    void send(uint8_t* serialisedMsg, size_t msgSize, bool more = false);

    // If known, pass a size parameter to pre-allocate a recv buffer
    Message recv(int size = 0);

    // The MessageEndpointServer needs direct access to the socket
    std::unique_ptr<zmq::socket_t> socket;

    std::string getHost();

    int getPort();

    void setRecvTimeoutMs(int value);

    void setSendTimeoutMs(int value);

  protected:
    const std::string host;
    const int port;
    const std::string address;
    std::thread::id tid;
    int id;

    int recvTimeoutMs = DEFAULT_RECV_TIMEOUT_MS;
    int sendTimeoutMs = DEFAULT_SEND_TIMEOUT_MS;

    void validateTimeout(int value);

    Message recvBuffer(int size);

    Message recvNoBuffer();
};

/* Send and Recv Message Endpoints */

class SendMessageEndpoint : public MessageEndpoint
{
  public:
    SendMessageEndpoint(const std::string& hostIn, int portIn);

    void open(MessageContext& context);

    void close();
};

class RecvMessageEndpoint : public MessageEndpoint
{
  public:
    RecvMessageEndpoint(int portIn);

    void open(MessageContext& context);

    void close();
};

class MessageTimeoutException : public faabric::util::FaabricException
{
  public:
    explicit MessageTimeoutException(std::string message)
      : FaabricException(std::move(message))
    {}
};
}
