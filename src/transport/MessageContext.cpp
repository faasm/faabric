#include <faabric/transport/MessageContext.h>

namespace faabric::transport {
MessageContext::MessageContext()
  : ctx(1)
{}

MessageContext::MessageContext(int overrideIoThreads)
  : ctx(overrideIoThreads)
{}

MessageContext::~MessageContext()
{
    this->close();
}

zmq::context_t& MessageContext::get()
{
    return this->ctx;
}

void MessageContext::close()
{
    this->ctx.close();
}
}
