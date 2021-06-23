#pragma once

#include <shared_mutex>
#include <zmq.hpp>

#define ZMQ_CONTEXT_IO_THREADS 1

namespace faabric::transport {

void initGlobalMessageContext();

std::shared_ptr<zmq::context_t> getGlobalMessageContext();

void closeGlobalMessageContext();

}
