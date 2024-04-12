#pragma once

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
void setTimeoutMs(int connFd, int timeoutMs);
}
