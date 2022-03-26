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
    explicit Message(std::unique_ptr<zmq::message_t> msgIn);

    explicit Message(int sizeIn);

    // Empty message signals shutdown
    Message() = default;

    char* data();

    uint8_t* udata();

    std::vector<uint8_t> dataCopy();

    int size();

    bool more();

  private:
    std::unique_ptr<zmq::message_t> msg;

    std::vector<uint8_t> bytes;

    bool _more = false;
};
}
