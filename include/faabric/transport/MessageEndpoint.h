#pragma once

#include <google/protobuf/message.h>

#include <faabric/util/logging.h>

#include <zmq.hpp>

namespace faabric::transport {
enum class ZeroMQSocketType { PUSH, PULL };

// Wrapper around zmq::context_t. Note that the ZMQ context is thread-safe.
class MessageContext
{
  public:
    MessageContext();

    // Message context should not be copied as there must only be one ZMQ context
    MessageContext(const MessageContext& ctx) = delete;

    // As a rule of thumb, use one IO thread per Gbps of data
    MessageContext(int overrideIoThreads);

    ~MessageContext();

    zmq::context_t& get();

    void close();

    zmq::context_t ctx;
};

class MessageEndpoint
{
  public:
    MessageEndpoint(const std::string& hostIn, int portIn);

    ~MessageEndpoint();

    void start(faabric::transport::MessageContext& context,
               faabric::transport::ZeroMQSocketType sockTypeIn,
               bool bind);

    void handleMessage();

    void close();

  protected:
    const std::string host;
    const int port;

    std::unique_ptr<zmq::socket_t> sock;
    faabric::transport::ZeroMQSocketType sockType;

    void sendMessage(char* serialisedMsg, size_t msgSize);

    virtual void doHandleMessage(const void* msgData, int size) = 0;
};
}
