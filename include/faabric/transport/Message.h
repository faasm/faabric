#pragma once

#include <span>
#include <string>
#include <vector>

#include <faabric/util/bytes.h>
#include <nng/nng.h>

// The header structure is:
// - Message code (uint8_t)
// - Message body size (uint64_t)
// - Message sequence number of in-order message delivery default -1 (int32_t)
#define NO_HEADER 0
#define HEADER_MSG_SIZE (sizeof(uint8_t) + sizeof(uint64_t) + sizeof(int32_t))

#define SHUTDOWN_HEADER 220
static constexpr std::array<uint8_t, 4> shutdownPayload = { 0, 0, 1, 1 };

#define NO_SEQUENCE_NUM -1

namespace faabric::transport {

/**
 * Types of message send/ receive outcomes.
 */
enum class MessageResponseCode
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
class Message final
{
  public:
    Message(size_t bufferSize);

    Message(nng_msg* nngMsg);

    Message(MessageResponseCode responseCodeIn);

    ~Message();

    // Delete everything copy-related, custom move constructors to reset the
    // original object on move.
    Message(const Message& other) = delete;

    Message& operator=(const Message& other) = delete;

    // Inline for better codegen
    Message(Message&& other) { this->operator=(std::move(other)); }

    Message& operator=(Message&& other)
    {
        other.nngMsg = nngMsg;
        other.responseCode = responseCode;
        other._header = _header;
        other._sequenceNum = _sequenceNum;
        nngMsg = nullptr;
        responseCode = MessageResponseCode::SUCCESS;
        _header = 0;
        _sequenceNum = NO_SEQUENCE_NUM;
        return *this;
    }

    MessageResponseCode getResponseCode() { return responseCode; }

    // Includes the header
    std::span<uint8_t> allData()
    {
        return nngMsg == nullptr
                 ? std::span<uint8_t>()
                 : std::span<uint8_t>(
                     reinterpret_cast<uint8_t*>(nng_msg_body(nngMsg)),
                     nng_msg_len(nngMsg));
    }

    std::span<const uint8_t> allData() const
    {
        return nngMsg == nullptr
                 ? std::span<const uint8_t>()
                 : std::span<const uint8_t>(
                     reinterpret_cast<const uint8_t*>(nng_msg_body(nngMsg)),
                     nng_msg_len(nngMsg));
    }

    std::span<char> data();

    std::span<uint8_t> udata();

    std::vector<uint8_t> dataCopy() const;

    uint8_t getHeader() const
    {
        return nngMsg == nullptr ? 0 : allData().data()[0];
    }

    uint64_t getDeclaredDataSize() const
    {
        return faabric::util::unalignedRead<uint64_t>(allData().data() +
                                                      sizeof(uint8_t));
    }

    int getSequenceNum() const
    {
        return allData().size() < HEADER_MSG_SIZE
                 ? NO_SEQUENCE_NUM
                 : faabric::util::unalignedRead<int32_t>(
                     allData().data() + sizeof(uint8_t) + sizeof(uint64_t));
    }

  private:
    nng_msg* nngMsg = nullptr;

    MessageResponseCode responseCode = MessageResponseCode::SUCCESS;

    uint8_t _header = 0;

    int _sequenceNum = NO_SEQUENCE_NUM;
};
}
