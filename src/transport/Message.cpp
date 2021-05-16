#include <faabric/transport/Message.h>

namespace faabric::transport {
Message::Message(const zmq::message_t& msgIn)
  : msg(reinterpret_cast<const uint8_t*>(msgIn.data()), msgIn.size())
  , _more(msgIn.more())
{}

char* Message::data()
{
    return reinterpret_cast<char*>(msg.data());
}

uint8_t* Message::udata()
{
    return msg.data();
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
