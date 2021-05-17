#include <faabric/transport/Message.h>
#include <faabric/util/logging.h>

namespace faabric::transport {
Message::Message(const zmq::message_t& msgIn)
  : _size(msgIn.size())
  , _more(msgIn.more())
{
    // TODO - never freeing this
    faabric::util::getLogger()->warn("allocating memory");
    msg = reinterpret_cast<uint8_t*>(malloc(_size * sizeof(uint8_t)));
    memcpy(msg, msgIn.data(), _size);
}

Message::Message(int sizeIn)
  : _size(sizeIn)
  , _more(false)
{
    // TODO - never freeing this
    faabric::util::getLogger()->warn("allocating memory");
    msg = reinterpret_cast<uint8_t*>(malloc(_size * sizeof(uint8_t)));
}

Message::Message(Message& msg)
  : msg(msg.udata())
  , _size(msg.size())
  , _more(msg.more())
{
    faabric::util::getLogger()->warn("calling overloaded copy");
}

Message& Message::operator=(Message& message)
{
    faabric::util::getLogger()->warn("calling overloaded assignment");
    // Check for self-assignment
    if (this == &message) {
        return *this;
    }

    _size = message.size();
    _more = message.more();
    msg = message.udata();

    return *this;
}

char* Message::data()
{
    return reinterpret_cast<char*>(msg);
}

uint8_t* Message::udata()
{
    return msg;
}

int Message::size()
{
    return _size;
}

bool Message::more()
{
    return _more;
}
}
