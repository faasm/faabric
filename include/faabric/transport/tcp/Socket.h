#pragma once

#include <arpa/inet.h>
#include <atomic>
#include <functional>
#include <memory>
#include <sys/socket.h>

namespace faabric::transport::tcp {
class Socket
{
  public:
    Socket();
    Socket(int connFd);
    ~Socket();

    int get();

  private:
    int connFd;
};
}
