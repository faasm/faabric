#include <faabric/transport/tcp/SocketOptions.h>
#include <faabric/util/logging.h>

#include <fcntl.h>
#include <sys/socket.h>

namespace faabric::transport::tcp {
void reuseAddr(int connFd)
{
    // TODO: do we need to check the value populated in enable?
    int enable = 1;

    int ret =
      setsockopt(connFd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int));
    if (ret < 0) {
        SPDLOG_ERROR("setsockopt(SO_REUSEADDR) failed on fd: {}", connFd);
        throw std::runtime_error("Error setting socket option: SO_REUSADDR");
    }
}

void setNonBlocking(int connFd)
{
    int flags = fcntl(connFd, F_GETFL, 0);
    if (flags < 0) {
        SPDLOG_ERROR("fcntl(fd, F_GETFL, 0) failed on fd: {}", connFd);
        throw std::runtime_error("Error setting socket as non-blocking");
    }

    // TODO: check return value?
    fcntl(connFd, F_SETFL, flags | O_NONBLOCK);
}

void setBlocking(int connFd)
{
    int flags = fcntl(connFd, F_GETFL, 0);
    if (flags < 0) {
        SPDLOG_ERROR("fcntl(fd, F_GETFL, 0) failed on fd: {}", connFd);
        throw std::runtime_error("Error setting socket as blocking");
    }

    // TODO: check return value?
    fcntl(connFd, F_SETFL, flags & ~O_NONBLOCK);
}

bool isNonBlocking(int connFd)
{
    int flags = fcntl(connFd, F_GETFL, 0);
    if (flags < 0) {
        SPDLOG_ERROR("fcntl(fd, F_GETFL, 0) failed on fd: {}", connFd);
        throw std::runtime_error("Error checking if socket is blocking");
    }

    return (flags & O_NONBLOCK) != 0;
}
}
