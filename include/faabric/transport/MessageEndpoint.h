#pragma once

#include <google/protobuf/message.h>

#include <faabric/transport/Message.h>
#include <faabric/transport/MessageContext.h>
#include <faabric/util/logging.h>

#include <thread>
#include <zmq.hpp>

// Defined in libzmq/include/zmq.h
#define ZMQ_ETERM ETERM

#define ANY_HOST "0.0.0.0"

namespace faabric::transport {
void print_trace(void);

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

    void close();

    void send(uint8_t* serialisedMsg, size_t msgSize, bool more = false);

    // If known, pass a size parameter to pre-allocate a recv buffer
    Message recv(int size = 0);

    // The MessageEndpointServer needs direct access to the socket
    std::unique_ptr<zmq::socket_t> socket;

    std::string getHost();

    int getPort();

  protected:
    const std::string host;
    const int port;
    std::thread::id tid;

    // TODO -remove
    int id;
};

/* Send and Recv Message Endpoints */

class SendMessageEndpoint : public MessageEndpoint
{
  public:
    SendMessageEndpoint(const std::string& hostIn, int portIn);

    void open(MessageContext& context);
};

class RecvMessageEndpoint : public MessageEndpoint
{
  public:
    RecvMessageEndpoint(int portIn);

    void open(MessageContext& context);
};
}
