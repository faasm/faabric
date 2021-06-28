#pragma once

#include <shared_mutex>
#include <zmq.hpp>

// We specify a number of background I/O threads when constructing the ZeroMQ
// context. Guidelines on how to scale this can be found here:
// https://zguide.zeromq.org/docs/chapter2/#I-O-Threads

#define ZMQ_CONTEXT_IO_THREADS 1

namespace faabric::transport {

void initGlobalMessageContext();

std::shared_ptr<zmq::context_t> getGlobalMessageContext();

void closeGlobalMessageContext();

}
