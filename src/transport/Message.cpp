#include <faabric/transport/Message.h>
#include <faabric/util/macros.h>

namespace faabric::transport {

Message::Message(size_t size)
  : buffer(size)
{}

Message::Message(MessageResponseCode responseCodeIn)
  : responseCode(responseCodeIn)
{}

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
