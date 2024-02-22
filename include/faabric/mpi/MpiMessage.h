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

struct MpiMessage
{
    int32_t id;
    int32_t worldId;
    int32_t sendRank;
    int32_t recvRank;
    int32_t typeSize;
    int32_t count;
    MpiMessageType messageType;
    void* buffer;
};

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
