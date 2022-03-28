#pragma once

#include <string>
#include <zmq.hpp>

namespace faabric::transport {

/**
 * Types of message send/ receive outcomes.
 */
enum MessageResponseCode
{
    SUCCESS,
    TERM,
    TIMEOUT,
    ERROR
};

/**
 * Represents message data passed around the transport layer. Essentially an
 * array of bytes, with a size and a flag to say whether there's more data to
 * follow.
 *
 * We must block users from accidentally copying these messages. If they need to
 * be passed around, it should be done using move semantics/ constructors.
 */
class Message
{
  public:
    explicit Message(size_t size);

    explicit Message(Message&& other) noexcept;

    explicit Message(MessageResponseCode responseCodeIn);

    Message& operator=(Message&&);

    MessageResponseCode getResponseCode() { return responseCode; }

    char* data();

    uint8_t* udata();

    std::vector<uint8_t> dataCopy();

    int size();

    void setHeader(uint8_t header) { _header = header; };

    uint8_t getHeader() { return _header; };

  private:
    std::vector<uint8_t> buffer;

    MessageResponseCode responseCode = MessageResponseCode::SUCCESS;

    uint8_t _header = 0;
};
}
