#include <faabric/transport/Message.h>
#include <faabric/util/macros.h>

namespace faabric::transport {

Message::Message(zmq::message_t msgIn)
  : msg(std::move(msgIn))
  , _more(msg.more())
{}

Message::Message(int sizeIn)
  : bytes(sizeIn)
  , _more(false)
{}

char* Message::data()
{
    if (!msg.empty()) {
        return reinterpret_cast<char*>(msg.data());
    }
    return reinterpret_cast<char*>(bytes.data());
}

uint8_t* Message::udata()
{
    if (!msg.empty()) {
        return reinterpret_cast<uint8_t*>(msg.data());
    }

    return bytes.data();
}

std::vector<uint8_t> Message::dataCopy()
{
    if (!msg.empty()) {
        return std::vector<uint8_t>(BYTES(msg.data()),
                                    BYTES(msg.data()) + msg.size());
    }

    return bytes;
}

int Message::size()
{
    if (!msg.empty()) {
        return msg.size();
    }
    return bytes.size();
}

bool Message::more()
{
    return _more;
}
}
