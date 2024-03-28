#pragma once

namespace faabric::transport::tcp {
void reuseAddr(int connFd);

void setNonBlocking(int connFd);
void setBlocking(int connFd);

bool isNonBlocking(int connFd);
}
