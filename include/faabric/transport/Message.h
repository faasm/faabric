#pragma once

#include <string>
#include <zmq.hpp>

namespace faabric::transport {
/**
 * Represents message data passed around the transport layer. Essentially an
 * array of bytes, with a size and a flag to say whether there's more data to
 * follow.
 *
 * Currently it's just a thin wrapper around a ZeroMQ message that avoids us
 * being dependent on the ZeroMQ API outside of boilerplate code.
 */
class Message
{
  public:
    /**
     * Creates a message from a ZeroMQ message. Importantly avoids a copy by
     * using the move constructor of zmq::message_t
     *
     * https://github.com/zeromq/cppzmq/blob/master/zmq.hpp#L408
     */
    explicit Message(zmq::message_t&& msgIn);

    explicit Message(Message&& other) noexcept;

    Message& operator=(Message&&);

    /**
     * Empty messages imply a failure or shutdown request
     */
    Message() = default;

    bool empty();

    char* data();

    uint8_t* udata();

    std::vector<uint8_t> dataCopy();

    int size();

    bool more();

  private:
    zmq::message_t msg;

    bool _more = false;
};
}
