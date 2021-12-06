#include <faabric/transport/Message.h>
#include <faabric/util/macros.h>

namespace faabric::transport {
Message::Message(const zmq::message_t& msgIn)
  : _more(msgIn.more())
{
    if (msgIn.data() != nullptr) {
        bytes = std::vector(BYTES_CONST(msgIn.data()),
                            BYTES_CONST(msgIn.data()) + msgIn.size());
    }
}

Message::Message(int sizeIn)
  : bytes(sizeIn)
  , _more(false)
{}

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
