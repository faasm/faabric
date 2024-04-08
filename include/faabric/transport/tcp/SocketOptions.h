#pragma once

namespace faabric::transport::tcp {
void setReuseAddr(int connFd);
void setNoDelay(int connFd);
void setQuickAck(int connFd);

void setNonBlocking(int connFd);
void setBlocking(int connFd);

bool isNonBlocking(int connFd);
}
