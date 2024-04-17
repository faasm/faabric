#pragma once

#include <cstddef>

namespace faabric::transport::tcp {
void setReuseAddr(int connFd);
void setNoDelay(int connFd);
void setQuickAck(int connFd);

// Blocking/Non-blocking sockets
void setNonBlocking(int connFd);
void setBlocking(int connFd);
bool isNonBlocking(int connFd);

// Enable busy polling for non-blocking sockets
void setBusyPolling(int connFd);

// Set timeout for blocking sockets
void setRecvTimeoutMs(int connFd, int timeoutMs);
void setSendTimeoutMs(int connFd, int timeoutMs);

// Set send/recv buffer sizes (important to guarantee MPI progress). Note that
// this options can never exceed the values set in net.core.{r,w}mem_max. To
// this extent, this functions must be used in conjunction with the adequate
// TCP configuration
void setRecvBufferSize(int connFd, size_t bufferSizeBytes);
void setSendBufferSize(int connFd, size_t bufferSizeBytes);
}
