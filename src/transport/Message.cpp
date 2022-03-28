#include <faabric/transport/Message.h>
#include <faabric/util/macros.h>

namespace faabric::transport {

Message::Message(size_t size):
    buffer(size) {
}

Message::Message(Message&& other) noexcept
  : buffer(std::move(other.buffer))
{}

Message::Message(MessageResponseCode responseCodeIn)
  : responseCode(responseCodeIn)
{}

Message& Message::operator=(Message&& other)
{
    buffer = std::move(other.buffer);

    return *this;
}

char* Message::data()
{
    return reinterpret_cast<char*>(buffer.data());
}

uint8_t* Message::udata()
{
    return buffer.data();
}

std::vector<uint8_t> Message::dataCopy()
{
    return std::vector<uint8_t>(buffer.begin(), buffer.end());
}

int Message::size()
{
    return buffer.size();
}
}
