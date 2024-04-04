#pragma once

#include <faabric/transport/common.h>
#include <faabric/transport/tcp/Address.h>
#include <faabric/transport/tcp/Socket.h>

#include <deque>

namespace faabric::transport::tcp {
class RecvSocket
{
  public:
    RecvSocket(int port, const std::string& host = ANY_HOST);
    RecvSocket(const RecvSocket& recvSocket) = delete;
    ~RecvSocket();

    // Start BIND socket and mark it as a passive socket
    void listen();

    // Accept a connection into the socket, and return the connection fd
    int accept();

    // Receive bytes from a connection
    void recvOne(int conn, uint8_t* buffer, size_t bufferSize);

  private:
    Address addr;
    Socket sock;

    std::string host;
    int port;

    std::deque<int> openConnections;
};
}
