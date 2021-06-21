#pragma once

#include <shared_mutex>
#include <zmq.hpp>

namespace faabric::transport {
/* Wrapper around zmq::context_t
 *
 * The context object is thread safe, and the constructor parameter indicates
 * the number of hardware IO threads to be used. As a rule of thumb, use one
 * IO thread per Gbps of data.
 */
class MessageContext
{
  public:
    MessageContext();

    // Message context should not be copied as there must only be one ZMQ
    // context
    MessageContext(const MessageContext& ctx) = delete;

    MessageContext(int overrideIoThreads);

    ~MessageContext();

    zmq::context_t& getZMQContext();

    static std::shared_ptr<MessageContext> getInstance();

  private:
    zmq::context_t ctx;

    static std::shared_ptr<MessageContext> instance;
    static std::shared_mutex mx;
};

std::shared_ptr<MessageContext> getGlobalMessageContext();
}
