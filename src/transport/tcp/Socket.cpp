#include <faabric/transport/tcp/Socket.h>
#include <faabric/util/logging.h>

#include <cstring>
#include <sys/socket.h>

namespace faabric::transport::tcp {
Socket::Socket()
  : connFd(socket(AF_INET, SOCK_STREAM, 0))
{
    if (get() <= 0) {
        SPDLOG_ERROR("Failed to create TCP socket: {}", std::strerror(errno));
        throw std::runtime_error("Failed to create TCP socket");
    }
}

Socket::Socket(int conn)
  : connFd(conn)
{}

Socket::~Socket()
{
    ::close(connFd);
}
}
