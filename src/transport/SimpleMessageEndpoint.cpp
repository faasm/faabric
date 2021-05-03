#include <faabric/transport/SimpleMessageEndpoint.h>
#include <faabric/util/logging.h>

namespace faabric::transport {
SimpleMessageEndpoint::SimpleMessageEndpoint(const std::string& host, int port)
  : MessageEndpoint(host, port)
{}

SimpleMessageEndpoint::~SimpleMessageEndpoint()
{
    ;
    /*
    if (this->msgData) {
        delete[] this->msgData;
    }
    */
}

void SimpleMessageEndpoint::recv(char*& msgData, int& size)
{
    MessageEndpoint::recv();
    msgData = this->msgData;
    size = this->msgSize;
}

void SimpleMessageEndpoint::doRecv(void* msgData, int size) {
    /*
    if (this->msgData) {
        delete[] this->msgData;
    }
    */
    this->msgData = new char[size]();
    memcpy(this->msgData, (char*) msgData, size);
    this->msgSize = size;
}
}
