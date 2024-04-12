#pragma once

#include <arpa/inet.h>
#include <sys/socket.h>

namespace faabric::transport::tcp {

const int SocketTimeoutMs = 3000;

class Socket
{
  public:
    Socket();
    Socket(int connFd);
    Socket(const Socket& socket) = delete;
    ~Socket();

    int get() const { return connFd; }

  private:
    int connFd;
};
}
