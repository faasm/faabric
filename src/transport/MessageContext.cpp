#include <faabric/transport/MessageContext.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>

namespace faabric::transport {

std::shared_ptr<MessageContext> MessageContext::instance = nullptr;
std::shared_mutex MessageContext::mx;

MessageContext::MessageContext()
  : ctx(1)
{}

MessageContext::MessageContext(int overrideIoThreads)
  : ctx(overrideIoThreads)
{}

MessageContext::~MessageContext()
{
    SPDLOG_TRACE("Closing global ZeroMQ message context");
    this->ctx.close();
}

zmq::context_t& MessageContext::getZMQContext()
{
    return this->ctx;
}

std::shared_ptr<MessageContext> MessageContext::getInstance()
{
    if (instance == nullptr) {
        faabric::util::FullLock lock(mx);
        if (instance == nullptr) {
            instance = std::make_unique<MessageContext>();
        }
    }

    {
        faabric::util::SharedLock lock(mx);

        return instance;
    }
}

std::shared_ptr<MessageContext> getGlobalMessageContext()
{
    return MessageContext::getInstance();
}
}
