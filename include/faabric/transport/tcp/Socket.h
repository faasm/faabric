#pragma once

#include <arpa/inet.h>
#include <sys/socket.h>

namespace faabric::transport::tcp {

const int SocketTimeoutMs = 5000;
// We get this value from OpenMPI's recommended TCP settings (FAQ 9):
// https://www.open-mpi.org/faq/?category=tcp
const size_t SocketBufferSizeBytes = 16777216;

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
