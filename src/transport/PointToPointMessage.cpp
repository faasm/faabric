#include <faabric/transport/PointToPointMessage.h>
#include <faabric/util/memory.h>

#include <cassert>
#include <cstring>
#include <span>

namespace faabric::transport {

void serializePtpMsg(std::span<uint8_t> buffer, const PointToPointMessage& msg)
{
    assert(buffer.size() == sizeof(PointToPointMessage) + msg.dataSize);
    std::memcpy(buffer.data(), &msg, sizeof(PointToPointMessage));

    if (msg.dataSize > 0 && msg.dataPtr != nullptr) {
        std::memcpy(buffer.data() + sizeof(PointToPointMessage),
                    msg.dataPtr,
                    msg.dataSize);
    }
}

// Parse all the fixed-size parts of the struct
static void parsePtpMsgCommon(std::span<const uint8_t> bytes,
                              PointToPointMessage* msg)
{
    assert(msg != nullptr);
    assert(bytes.size() >= sizeof(PointToPointMessage));
    std::memcpy(msg, bytes.data(), sizeof(PointToPointMessage));
    size_t thisDataSize = bytes.size() - sizeof(PointToPointMessage);
    assert(thisDataSize == msg->dataSize);

    if (thisDataSize == 0) {
        msg->dataPtr = nullptr;
    }
}

void parsePtpMsg(std::span<const uint8_t> bytes, PointToPointMessage* msg)
{
    parsePtpMsgCommon(bytes, msg);

    if (msg->dataSize == 0) {
        return;
    }

    // malloc memory for the PTP message payload
    msg->dataPtr = faabric::util::malloc(msg->dataSize);
    std::memcpy(
      msg->dataPtr, bytes.data() + sizeof(PointToPointMessage), msg->dataSize);
}

void parsePtpMsg(std::span<const uint8_t> bytes,
                 PointToPointMessage* msg,
                 std::span<uint8_t> preAllocBuffer)
{
    parsePtpMsgCommon(bytes, msg);

    assert(msg->dataSize == preAllocBuffer.size());
    msg->dataPtr = preAllocBuffer.data();
    std::memcpy(
      msg->dataPtr, bytes.data() + sizeof(PointToPointMessage), msg->dataSize);
}
}
