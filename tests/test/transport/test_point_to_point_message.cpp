#include <catch2/catch.hpp>

#include <faabric/transport/PointToPointMessage.h>
#include <faabric/util/memory.h>

#include <cstring>

using namespace faabric::transport;

namespace tests {

bool arePtpMsgEqual(const PointToPointMessage& msgA, const PointToPointMessage& msgB)
{
    // First, compare the message body (excluding the pointer, which we
    // know is at the end)
    if (std::memcmp(&msgA, &msgB, sizeof(PointToPointMessage) - sizeof(void*)) != 0) {
        return false;
    }

    // Check that if one buffer points to null, so must do the other
    if (msgA.dataPtr == nullptr || msgB.dataPtr == nullptr) {
        return msgA.dataPtr == msgB.dataPtr;
    }

    return std::memcmp(msgA.dataPtr, msgB.dataPtr, msgA.dataSize) == 0;
}

TEST_CASE("Test (de)serialising a PTP message", "[ptp]")
{
    PointToPointMessage msg({ .appId = 1,
                              .groupId = 2,
                              .sendIdx = 3,
                              .recvIdx = 4,
                              .dataSize = 0,
                              .dataPtr = nullptr });

    SECTION("Empty message")
    {
        msg.dataSize = 0;
        msg.dataPtr = nullptr;
    }

    SECTION("Non-empty message")
    {
        std::vector<int> nums = { 1, 2, 3, 4, 5, 6, 6 };
        msg.dataSize = nums.size() * sizeof(int);
        msg.dataPtr = faabric::util::malloc(msg.dataSize);
        std::memcpy(msg.dataPtr, nums.data(), msg.dataSize);
    }

    // Serialise and de-serialise
    std::vector<uint8_t> buffer(sizeof(PointToPointMessage) + msg.dataSize);
    serializePtpMsg(buffer, msg);

    PointToPointMessage parsedMsg;
    parsePtpMsg(buffer, &parsedMsg);

    REQUIRE(arePtpMsgEqual(msg, parsedMsg));

    if (msg.dataPtr != nullptr) {
        faabric::util::free(msg.dataPtr);
    }
    if (parsedMsg.dataPtr != nullptr) {
        faabric::util::free(parsedMsg.dataPtr);
    }
}

TEST_CASE("Test (de)serialising a PTP message into prealloc buffer", "[ptp]")
{
    PointToPointMessage msg({ .appId = 1,
                              .groupId = 2,
                              .sendIdx = 3,
                              .recvIdx = 4,
                              .dataSize = 0,
                              .dataPtr = nullptr });

    std::vector<int> nums = { 1, 2, 3, 4, 5, 6, 6 };
    msg.dataSize = nums.size() * sizeof(int);
    msg.dataPtr = faabric::util::malloc(msg.dataSize);
    std::memcpy(msg.dataPtr, nums.data(), msg.dataSize);

    // Serialise and de-serialise
    std::vector<uint8_t> buffer(sizeof(PointToPointMessage) + msg.dataSize);
    serializePtpMsg(buffer, msg);

    std::vector<uint8_t> preAllocBuffer(msg.dataSize);
    PointToPointMessage parsedMsg;
    parsePtpMsg(buffer, &parsedMsg, preAllocBuffer);

    REQUIRE(arePtpMsgEqual(msg, parsedMsg));
    REQUIRE(parsedMsg.dataPtr == preAllocBuffer.data());

    faabric::util::free(msg.dataPtr);
}
}
