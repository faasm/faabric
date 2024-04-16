#pragma once

#include <faabric/transport/tcp/Address.h>
#include <faabric/transport/tcp/Socket.h>
#include <faabric/util/logging.h>

#include <cstring>
#include <sys/uio.h>

namespace faabric::transport::tcp {
class SendSocket
{
  public:
    SendSocket(const std::string& host, int port);

    void dial();

    void sendOne(const uint8_t* buffer, size_t bufferSize);

  private:
    Address addr;
    Socket sock;
    bool connected;

    std::string host;
    int port;

    void setSocketOptions(int connFd);
};
}
