#include <faabric/transport/MessageContext.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>

namespace faabric::transport {

static std::unique_ptr<MessageContext> instance = nullptr;
static std::shared_mutex messageContextMx;

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

zmq::context_t& MessageContext::get()
{
    return this->ctx;
}

faabric::transport::MessageContext& getGlobalMessageContext()
{
    if (instance == nullptr) {
        faabric::util::FullLock lock(messageContextMx);
        if (instance == nullptr) {
            instance = std::make_unique<MessageContext>();
        }
    }

    {
        faabric::util::SharedLock lock(messageContextMx);

        return *instance;
    }
}
}
