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

void MessageContext::close()
{
    this->ctx.close();
    this->isContextShutDown = true;
}

zmq::context_t& MessageContext::get()
{
    return this->ctx;
}

faabric::transport::MessageContext& getGlobalMessageContext()
{
    static auto msgContext =
      std::make_unique<faabric::transport::MessageContext>();
    // The message context needs to be opened and closed every server instance.
    // Sometimes (e.g. tests) the scheduler is re-used, but the message context
    // needs to be reset. In this situations we override the shut-down message
    // context.
    if (msgContext->isContextShutDown) {
        msgContext =
          std::move(std::make_unique<faabric::transport::MessageContext>());
        msgContext->isContextShutDown = false;
    }
    return *msgContext;
}
}
