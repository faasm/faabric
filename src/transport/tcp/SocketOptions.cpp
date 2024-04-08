#include <faabric/transport/tcp/SocketOptions.h>
#include <faabric/util/logging.h>

#include <fcntl.h>
#include <netinet/in.h>  // IPPROTO_TCP
#include <netinet/tcp.h> // TCP_NODELAY, TCP_QUICKACK
#include <sys/socket.h>

namespace faabric::transport::tcp {
void setReuseAddr(int connFd)
{
    // TODO: do we need to check the value populated in opt?
    int opt = 1;
    int ret = setsockopt(connFd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    if (ret < 0) {
        SPDLOG_ERROR("setsockopt(SO_REUSEADDR) failed on fd: {}", connFd);
        throw std::runtime_error("Error setting socket option: SO_REUSADDR");
    }
}

void setNoDelay(int connFd)
{
    int opt = 1;
    int ret = setsockopt(connFd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
    if (ret < 0) {
        SPDLOG_ERROR("setsockopt(TCP_NODELAY) failed on fd: {}", connFd);
    }
}

void setQuickAck(int connFd)
{
    int opt = 1;
    int ret = setsockopt(connFd, IPPROTO_TCP, TCP_QUICKACK, &opt, sizeof(opt));
    if (ret < 0) {
        SPDLOG_ERROR("setsockopt(TCP_QUICKACK) failed on fd: {}", connFd);
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
