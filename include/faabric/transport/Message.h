#pragma once

#include <string>
#include <zmq.hpp>

namespace faabric::transport {
/* Wrapper arround zmq::message_t
 *
 * Thin abstraction around 0MQ's message type. Represents an array of bytes,
 * its size, and other traits from the underlying type useful to faabric.
 */
class Message
{
  public:
    Message(const zmq::message_t& msg);

    char* data();

    uint8_t* udata();

    int size();

    bool more();

  private:
    std::basic_string<uint8_t> msg;
    bool _more;
};
}
