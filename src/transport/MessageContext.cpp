#include <faabric/transport/MessageContext.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>

namespace faabric::transport {

static std::shared_ptr<zmq::context_t> instance = nullptr;
static std::shared_mutex mx;

std::shared_ptr<zmq::context_t> getGlobalMessageContext()
{
    if (instance == nullptr) {
        faabric::util::FullLock lock(mx);
        if (instance == nullptr) {
            instance = std::make_shared<zmq::context_t>(ZMQ_CONTEXT_IO_THREADS);
        }
    }

    {
        faabric::util::SharedLock lock(mx);

        return instance;
    }
}
}
