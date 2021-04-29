#pragma once

#include <google/protobuf/message.h>

#include <faabric/util/logging.h>

#include <atomic>
#include <zmq.hpp>

namespace faabric::transport {
enum class SocketType { PUSH, PULL };

/* Wrapper arround zmq::socket_t 
 *
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

    void recv();

    void close();

  protected:
    const std::string host;
    const int port;

    void send(char* serialisedMsg, size_t msgSize);

    virtual void doRecv(const void* msgData, int size) = 0;

  private:
    zmq::socket_t socket;
};
}
