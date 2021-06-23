#include <faabric/transport/context.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>

namespace faabric::transport {

/*
 * The zmq::context_t object is thread safe, and the constructor parameter
 * indicates the number of hardware IO threads to be used. As a rule of thumb,
 * use one IO thread per Gbps of data.
 */

static std::shared_ptr<zmq::context_t> instance = nullptr;

void initGlobalMessageContext()
{
    if (instance != nullptr) {
        throw std::runtime_error("Trying to initialise ZeroMQ context twice");
    }

    SPDLOG_TRACE("Initialising global ZeroMQ context");
    instance = std::make_shared<zmq::context_t>(ZMQ_CONTEXT_IO_THREADS);
}

std::shared_ptr<zmq::context_t> getGlobalMessageContext()
{
    if (instance == nullptr) {
        throw std::runtime_error("Trying to access uninitialised ZeroMQ context");
    }

    return instance;
}

void closeGlobalMessageContext()
{
    if (instance == nullptr) {
        throw std::runtime_error("Cannot close uninitialised ZeroMQ context");
    }

    SPDLOG_TRACE("Destroying global ZeroMQ context");

    // Force outstanding ops to return ETERM
    instance->shutdown();

    // Close the context
    instance->close();
}
}
