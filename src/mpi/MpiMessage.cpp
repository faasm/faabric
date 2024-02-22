#include <faabric/mpi/MpiMessage.h>
#include <faabric/util/memory.h>

#include <cassert>
#include <cstdint>
#include <cstring>

namespace faabric::mpi {

void parseMpiMsg(const std::vector<uint8_t>& bytes, MpiMessage* msg)
{
    assert(msg != nullptr);
    assert(bytes.size() >= sizeof(MpiMessage));
    std::memcpy(msg, bytes.data(), sizeof(MpiMessage));
    size_t thisPayloadSize = bytes.size() - sizeof(MpiMessage);
    assert(thisPayloadSize == payloadSize(*msg));

    if (thisPayloadSize == 0) {
        msg->buffer = nullptr;
        return;
    }

    msg->buffer = faabric::util::malloc(thisPayloadSize);
    std::memcpy(
      msg->buffer, bytes.data() + sizeof(MpiMessage), thisPayloadSize);
}

void serializeMpiMsg(std::vector<uint8_t>& buffer, const MpiMessage& msg)
{
    std::memcpy(buffer.data(), &msg, sizeof(MpiMessage));
    size_t payloadSz = payloadSize(msg);
    if (payloadSz > 0 && msg.buffer != nullptr) {
        std::memcpy(buffer.data() + sizeof(MpiMessage), msg.buffer, payloadSz);
    }
}
}
