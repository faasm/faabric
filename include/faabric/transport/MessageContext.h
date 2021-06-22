#pragma once

#include <shared_mutex>
#include <zmq.hpp>

/*
 * The context object is thread safe, and the constructor parameter indicates
 * the number of hardware IO threads to be used. As a rule of thumb, use one
 * IO thread per Gbps of data.
 */
#define ZMQ_CONTEXT_IO_THREADS 1

namespace faabric::transport {
std::shared_ptr<zmq::context_t> getGlobalMessageContext();
}
