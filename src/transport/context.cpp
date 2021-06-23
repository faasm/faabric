#include <faabric/transport/context.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>

namespace faabric::transport {

/*
 * The zmq::context_t object is thread safe, and the constructor parameter
 * indicates the number of hardware IO threads to be used. As a rule of thumb,
 * use one IO thread per Gbps of data.
 */

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

void initGlobalMessageContext()
{
    if (instance != nullptr) {
        throw std::runtime_error("Must not initialise global context twice");
    }

    SPDLOG_TRACE("Initialising global ZeroMQ context");
    instance = std::make_shared<ContextWrapper>();
}

std::shared_ptr<zmq::context_t> getGlobalMessageContext()
{
    if (instance == nullptr) {
        throw std::runtime_error(
          "Must explicitly initialise and close global message context");
    }

    return instance->ctx;
}

void closeGlobalMessageContext()
{
    if (instance == nullptr) {
        throw std::runtime_error("Cannot close an uninitialised context");
    }

    SPDLOG_TRACE("Destroying global ZeroMQ context");

    // Force outstanding ops to return ETERM
    instance->ctx->shutdown();

    // Close the context
    instance->ctx->close();
}
}
