#include <faabric/transport/Message.h>

namespace faabric::transport {
Message::Message(const zmq::message_t& msgIn)
  : bytes(msgIn.size())
  , _more(msgIn.more())
{
    std::memcpy(bytes.data(), msgIn.data(), msgIn.size());
}

Message::Message(int sizeIn)
  : bytes(sizeIn)
  , _more(false)
{}

// Empty message signals shutdown
Message::Message() {}

char* Message::data()
{
    return reinterpret_cast<char*>(bytes.data());
}

uint8_t* Message::udata()
{
    return bytes.data();
}

std::vector<uint8_t> Message::dataCopy()
{
    return bytes;
}

int Message::size()
{
    return bytes.size();
}

bool Message::more()
{
    return _more;
}
}
