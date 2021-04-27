#pragma once

#include <faabric/util/logging.h>

#include <zmq.hpp>

namespace faabric::zeromq {
enum class ZeroMQSocketType { PUSH, PULL };
class MessageEndpoint
{
  public:
    MessageEndpoint(const std::string& hostIn, int portIn);

    ~MessageEndpoint();

    void start(zmq::context_t& context,
               faabric::zeromq::ZeroMQSocketType sockTypeIn,
               bool bind);

    void handleMessage();

  protected:
    const std::string host;
    const int port;

    // zmq::context_t context;
    std::unique_ptr<zmq::socket_t> sock;
    faabric::zeromq::ZeroMQSocketType sockType;

    void sendMessage(zmq::message_t& msg);

    virtual void doHandleMessage(zmq::message_t& msg) = 0;
};
}
