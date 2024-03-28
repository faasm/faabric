#pragma once

#include <netinet/in.h>
#include <string>
#include <sys/socket.h>

namespace faabric::transport::tcp {
class Address
{
  public:
    Address(const std::string& host, int port);

    Address(int port);

    sockaddr* get() const;

  private:
    sockaddr_in addr;
};

}
