#include <catch2/catch.hpp>

#include <faabric/mpi/MpiMessage.h>
#include <faabric/util/memory.h>

#include <cstring>

using namespace faabric::mpi;

namespace tests {

bool areMpiMsgEqual(const MpiMessage& msgA, const MpiMessage& msgB)
{
    auto sizeA = msgSize(msgA);
    auto sizeB = msgSize(msgB);

    if (sizeA != sizeB) {
        return false;
    }

    // First, compare the message body (excluding the pointer, which we
    // know is at the end)
    if (std::memcmp(&msgA, &msgB, sizeof(MpiMessage) - sizeof(void*)) != 0) {
        return false;
    }

    // Check that if one buffer points to null, so must do the other
    if (msgA.buffer == nullptr || msgB.buffer == nullptr) {
        return msgA.buffer == msgB.buffer;
    }

    // If none points to null, they must point to the same data
    auto payloadSizeA = payloadSize(msgA);
    auto payloadSizeB = payloadSize(msgB);
    // Assert, as this should pass given the previous comparisons
    assert(payloadSizeA == payloadSizeB);

    return std::memcmp(msgA.buffer, msgB.buffer, payloadSizeA) == 0;
}

TEST_CASE("Test getting a message size", "[mpi]")
{
    MpiMessage msg = { .id = 1,
                       .worldId = 3,
                       .sendRank = 3,
                       .recvRank = 7,
                       .typeSize = 1,
                       .count = 3,
                       .messageType = MpiMessageType::NORMAL };

    size_t expectedMsgSize = 0;
    size_t expectedPayloadSize = 0;

    SECTION("Empty message")
    {
        msg.buffer = nullptr;
        msg.count = 0;
        expectedMsgSize = sizeof(MpiMessage);
        expectedPayloadSize = 0;
    }

    SECTION("Non-empty message")
    {
        std::vector<int> nums = { 1, 2, 3, 4, 5, 6, 6 };
        msg.count = nums.size();
        msg.typeSize = sizeof(int);
        msg.buffer = faabric::util::malloc(msg.count * msg.typeSize);
        std::memcpy(msg.buffer, nums.data(), nums.size() * sizeof(int));

        expectedPayloadSize = sizeof(int) * nums.size();
        expectedMsgSize = sizeof(MpiMessage) + expectedPayloadSize;
    }

    REQUIRE(expectedMsgSize == msgSize(msg));
    REQUIRE(expectedPayloadSize == payloadSize(msg));

    if (msg.buffer != nullptr) {
        faabric::util::free(msg.buffer);
    }
}

TEST_CASE("Test (de)serialising an MPI message", "[mpi]")
{
    MpiMessage msg = { .id = 1,
                       .worldId = 3,
                       .sendRank = 3,
                       .recvRank = 7,
                       .typeSize = 1,
                       .count = 3,
                       .messageType = MpiMessageType::NORMAL };

    SECTION("Empty message")
    {
        msg.count = 0;
        msg.buffer = nullptr;
    }

    SECTION("Non-empty message")
    {
        std::vector<int> nums = { 1, 2, 3, 4, 5, 6, 6 };
        msg.count = nums.size();
        msg.typeSize = sizeof(int);
        msg.buffer = faabric::util::malloc(msg.count * msg.typeSize);
        std::memcpy(msg.buffer, nums.data(), nums.size() * sizeof(int));
    }

    // Serialise and de-serialise
    std::vector<uint8_t> buffer(msgSize(msg));
    serializeMpiMsg(buffer, msg);

    MpiMessage parsedMsg;
    parseMpiMsg(buffer, &parsedMsg);

    REQUIRE(areMpiMsgEqual(msg, parsedMsg));

    if (msg.buffer != nullptr) {
        faabric::util::free(msg.buffer);
    }
    if (parsedMsg.buffer != nullptr) {
        faabric::util::free(parsedMsg.buffer);
    }
}
}
