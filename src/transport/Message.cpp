#include <faabric/transport/Message.h>
#include <faabric/util/macros.h>

namespace faabric::transport {

Message::Message(zmq::message_t&& msgIn)
  : msg(std::move(msgIn))
  , _more(msg.more())
{}

Message::Message(Message&& other) noexcept
  : Message(std::move(other.msg))
{}

Message::Message(MessageResponseCode failCodeIn)
  : failCode(failCodeIn)
{}

Message& Message::operator=(Message&& other)
{
    msg.move(other.msg);

    return *this;
}

char* Message::data()
{
    return reinterpret_cast<char*>(msg.data());
}

uint8_t* Message::udata()
{
    return reinterpret_cast<uint8_t*>(msg.data());
}

std::vector<uint8_t> Message::dataCopy()
{
    return std::vector<uint8_t>(BYTES(msg.data()),
                                BYTES(msg.data()) + msg.size());
}

int Message::size()
{
    return msg.size();
}

bool Message::more()
{
    return _more;
}
}
