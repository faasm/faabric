#pragma once

#include <cstdint>
#include <vector>

// Constant copied from OpenMPI's SM implementation. It indicates the maximum
// number of Bytes that we may inline in a message (rather than malloc-ing)
// https://github.com/open-mpi/ompi/blob/main/opal/mca/btl/sm/btl_sm_component.c#L153
#define MPI_MAX_INLINE_SEND 256

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
    ALLTOALL_PACKED = 10,
    SENDRECV = 11,
    BROADCAST = 12,
    // Special message type for async messages that have not been unacked yet
    UNACKED_MPI_MESSAGE = 13,
    HANDSHAKE = 14,
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
    // This field is only used for async messages, but it helps making the
    // struct 8-aligned
    int32_t requestId;
    MpiMessageType messageType;
    union
    {
        void* buffer;
        uint8_t inlineMsg[MPI_MAX_INLINE_SEND];
    };
};
static_assert((sizeof(MpiMessage) % 8) == 0, "MPI message must be 8-aligned!");

inline size_t payloadSize(const MpiMessage& msg)
{
    return msg.typeSize * msg.count;
}

inline size_t msgSize(const MpiMessage& msg)
{
    size_t payloadSz = payloadSize(msg);

    // If we can inline the message, we do not need to add anything else
    if (payloadSz < MPI_MAX_INLINE_SEND) {
        return sizeof(MpiMessage);
    }

    return sizeof(MpiMessage) + payloadSz;
}

void serializeMpiMsg(std::vector<uint8_t>& buffer, const MpiMessage& msg);

void parseMpiMsg(const std::vector<uint8_t>& bytes, MpiMessage* msg);
}
