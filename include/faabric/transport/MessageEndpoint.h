#pragma once

#include <google/protobuf/message.h>

#include <faabric/transport/MessageContext.h>
#include <faabric/util/logging.h>

#include <atomic>
#include <zmq.hpp>

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

    // Message endpoints shouldn't be copied as ZeroMQ sockets are not thread
    // safe
    MessageEndpoint(const MessageEndpoint& ctx) = delete;

    ~MessageEndpoint();

    void open(faabric::transport::MessageContext& context,
              faabric::transport::SocketType sockTypeIn,
              bool bind);

    void close();

    void send(char* serialisedMsg, size_t msgSize, bool more = false);

    void sendFb(uint8_t* serialisedMsg, size_t msgSize, bool more = false);

    void recv();

    std::unique_ptr<zmq::socket_t> socket;

  protected:
    const std::string host;
    const int port;
    int id;

    virtual void doRecv(void* msgData, int size) = 0;
};
}
