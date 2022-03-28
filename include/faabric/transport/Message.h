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
 * Messages are not copyable, only movable, as they will regularly contain large
 * amounts of data.
 */
class Message
{
  public:
    // Delete everything copy-related, default everything move-related
    Message(const Message& other) = delete;

    Message& operator=(const Message& other) = delete;

    Message(Message&& other) = default;

    Message& operator=(Message&& other) = default;

    Message(size_t size);

    Message(MessageResponseCode responseCodeIn);

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
