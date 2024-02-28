#pragma once

#include <cstdint>
#include <span>

namespace faabric::transport {

/* Simple fixed-size C-struct to capture the state of a PTP message moving
 * through Faabric.
 *
 * We require fixed-size, and no unique pointers to be able to use
 * high-throughput ring-buffers to send the messages around. This also means
 * that we manually malloc/free the data pointer. The message size is:
 * 4 * int32_t = 4 * 4 bytes = 16 bytes
 * 1 * size_t = 1 * 8 bytes = 8 bytes
 * 1 * void* = 1 * 8 bytes = 8 bytes
 * total = 32 bytes = 4 * 8 so the struct is naturally 8 byte-aligned
 */
struct PointToPointMessage
{
    int32_t appId;
    int32_t groupId;
    int32_t sendIdx;
    int32_t recvIdx;
    size_t dataSize;
    void* dataPtr;
};
static_assert((sizeof(PointToPointMessage) % 8) == 0,
              "PTP message mus be 8-aligned!");

// The wire format for a PTP message is very simple: the fixed-size struct,
// followed by dataSize bytes containing the payload.
void serializePtpMsg(std::span<uint8_t> buffer, const PointToPointMessage& msg);

// This parsing function mallocs space for the message payload. This is to
// keep the PTP message at fixed-size, and be able to efficiently move it
// around in-memory queues
void parsePtpMsg(std::span<const uint8_t> bytes, PointToPointMessage* msg);

// Alternative signature for parsing PTP messages for when the caller can
// provide an already-allocated buffer to write into
void parsePtpMsg(std::span<const uint8_t> bytes,
                 PointToPointMessage* msg,
                 std::span<uint8_t> preAllocBuffer);
}
