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
    Message(const zmq::message_t& msgIn);

    Message(int sizeIn);

    Message(Message& msg);

    Message& operator=(Message& message);

    ~Message();

    char* data();

    uint8_t* udata();

    int size();

    bool more();

    void persist();

  private:
    uint8_t* msg;
    int _size;
    bool _more;
    bool _persist;
};
}
