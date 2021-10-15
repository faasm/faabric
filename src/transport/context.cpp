#include <faabric/transport/context.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>

namespace faabric::transport {

// The ZeroMQ context object is thread safe, so we're ok to have a single global
// instance.
static std::shared_ptr<zmq::context_t> instance = nullptr;

void initGlobalMessageContext()
{
    if (instance != nullptr) {
        SPDLOG_WARN("ZeroMQ context already initialised. Skipping");
        return;
    }

    SPDLOG_TRACE("Initialising global ZeroMQ context");
    instance = std::make_shared<zmq::context_t>(ZMQ_CONTEXT_IO_THREADS,
                                                32 * 1024 * 1024);
}

std::shared_ptr<zmq::context_t> getGlobalMessageContext()
{
    if (instance == nullptr) {
        throw std::runtime_error(
          "Trying to access uninitialised ZeroMQ context");
    }

    return instance;
}

void closeGlobalMessageContext()
{
    if (instance == nullptr) {
        SPDLOG_WARN(
          "ZeroMQ context already closed (or not initialised). Skipping");
        return;
    }

    SPDLOG_TRACE("Destroying global ZeroMQ context");

    // Force outstanding ops to return ETERM
    instance->shutdown();

    // Close the context
    instance->close();
}
}
