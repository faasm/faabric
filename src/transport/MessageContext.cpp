#include <faabric/transport/MessageContext.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>

namespace faabric::transport {

class ContextWrapper
{
  public:
    std::shared_ptr<zmq::context_t> ctx;

    ContextWrapper()
    {
        ctx = std::make_shared<zmq::context_t>(ZMQ_CONTEXT_IO_THREADS);
    }

    ~ContextWrapper()
    {
        SPDLOG_TRACE("Destroying ZeroMQ context");

        // Force outstanding ops to return ETERM
        ctx->shutdown();

        // Close the context
        ctx->close();
    }
};

static std::shared_ptr<ContextWrapper> instance = nullptr;
static std::shared_mutex mx;

std::shared_ptr<zmq::context_t> getGlobalMessageContext()
{
    if (instance == nullptr) {
        faabric::util::FullLock lock(mx);
        if (instance == nullptr) {
            instance = std::make_shared<ContextWrapper>();
        }
    }

    {
        faabric::util::SharedLock lock(mx);
        return instance->ctx;
    }
}
}
