#pragma once

#include <cstdint>
#include <vector>

namespace faabric::mpi {

enum MpiMessageType : int32_t
{
    NORMAL = 0,
    BARRIER_JOIN = 1,
    BARRIER_DONE = 2,
    SCATTER = 3,
    GATHER = 4,
    ALLGATHER = 5,
    REDUCE = 6,
    SCAN = 7,
    ALLREDUCE = 8,
    ALLTOALL = 9,
    SENDRECV = 10,
    BROADCAST = 11,
};

/* Simple fixed-size C-struct to capture the state of an MPI message moving
 * through Faabric.
 *
 * We require fixed-size, and no unique pointers to be able to use
 * high-throughput in-memory ring-buffers to send the messages around.
 * This also means that we manually malloc/free the data pointer. The message
 * size is:
 * 7 * int32_t = 7 * 4 bytes = 28 bytes
 * 1 * int32_t (padding) = 4 bytes
 * 1 * void* = 1 * 8 bytes = 8 bytes
 * total = 40 bytes = 5 * 8 so the struct is 8 byte-aligned
 */
struct MpiMessage
{
    int32_t id;
    int32_t worldId;
    int32_t sendRank;
    int32_t recvRank;
    int32_t typeSize;
    int32_t count;
    MpiMessageType messageType;
    int32_t __make_8_byte_aligned;
    void* buffer;
};
static_assert((sizeof(MpiMessage) % 8) == 0, "MPI message must be 8-aligned!");

inline size_t payloadSize(const MpiMessage& msg)
{
    return msg.typeSize * msg.count;
}

inline size_t msgSize(const MpiMessage& msg)
{
    return sizeof(MpiMessage) + payloadSize(msg);
}

void serializeMpiMsg(std::vector<uint8_t>& buffer, const MpiMessage& msg);

void parseMpiMsg(const std::vector<uint8_t>& bytes, MpiMessage* msg);
}
