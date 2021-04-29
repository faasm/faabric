#pragma once

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

    zmq::context_t& get();

    void close();

    zmq::context_t ctx;
};
}
