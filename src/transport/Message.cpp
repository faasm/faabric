#include <faabric/transport/Message.h>
#include <faabric/util/logging.h>

namespace faabric::transport {
Message::Message(const zmq::message_t& msgIn)
  : _size(msgIn.size())
  , _more(msgIn.more())
  , _persist(false)
{
    // TODO - never freeing this
    faabric::util::getLogger()->warn("allocating memory");
    msg = reinterpret_cast<uint8_t*>(malloc(_size * sizeof(uint8_t)));
    memcpy(msg, msgIn.data(), _size);
}

Message::Message(int sizeIn)
  : _size(sizeIn)
  , _more(false)
  , _persist(false)
{
    msg = reinterpret_cast<uint8_t*>(malloc(_size * sizeof(uint8_t)));
}

Message::Message(Message& msg)
{
    faabric::util::getLogger()->warn("calling overloaded copy");
}

Message::~Message()
{
    if (!_persist)
    {
        free(reinterpret_cast<void*>(msg));
    }
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

void Message::persist()
{
    assert(_persist == false);
    _persist = true;
}
}
