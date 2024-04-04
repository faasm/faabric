#include <faabric/transport/tcp/RecvSocket.h>
#include <faabric/transport/tcp/SocketOptions.h>
#include <faabric/util/logging.h>

namespace faabric::transport::tcp {
RecvSocket::RecvSocket(int port, const std::string& host)
  : addr(host, port)
  , host(host)
  , port(port)
{}

RecvSocket::~RecvSocket()
{
    for (const auto& openConnFd : openConnections) {
        ::close(openConnFd);
    }

    openConnections.clear();
}

void RecvSocket::listen()
{
    int connFd = sock.get();
    reuseAddr(connFd);
    if (!isNonBlocking(connFd)) {
        setNonBlocking(connFd);
    }

    SPDLOG_TRACE("Binding TCP socket to {}:{} (fd: {})", host, port, connFd);
    int ret = ::bind(connFd, addr.get(), sizeof(sockaddr_in));
    if (ret) {
        SPDLOG_ERROR("Error binding to {}:{} (fd: {}): {} (ret: {})",
                     host,
                     port,
                     connFd,
                     std::strerror(errno),
                     ret);
        throw std::runtime_error("Socket error binding to fd");
    }

    ret = ::listen(connFd, 1024);
    if (ret) {
        SPDLOG_ERROR("Error listening to {}:{} (fd: {}): {} (ret: {})",
                     host,
                     port,
                     connFd,
                     std::strerror(errno),
                     ret);
        throw std::runtime_error("Socket error listening to fd");
    }
}

int RecvSocket::accept()
{
    int connFd = sock.get();

    // Make accept blocking to make sure we account for races in initialisation
    setBlocking(connFd);
    int newConn = ::accept(sock.get(), 0, 0);
    if (newConn < 1) {
        SPDLOG_ERROR("Error accepting connection on {}:{} (fd: {}): {}",
                     host,
                     port,
                     connFd,
                     std::strerror(errno));
        throw std::runtime_error("Error accepting TCP connection");
    }
    setNonBlocking(connFd);

    // Set the newly accepted connection as blocking (we want `recv` to block)
    // TODO: do we want to block, or do we want to spinlock??
    setBlocking(newConn);

    // TODO: add constructor parameter of max num conn
    openConnections.push_back(newConn);

    return newConn;
}

void RecvSocket::recvOne(int conn, uint8_t* buffer, size_t bufferSize)
{
    size_t numRecvd = 0;

    while (numRecvd < bufferSize) {
        // Receive from socket
        int got = ::recv(conn, buffer, bufferSize - numRecvd, 0);
        if (got < 0) {
            // TODO: why?
            if (errno != EAGAIN) {
                SPDLOG_ERROR("TCP Server error receiving in {}: {}",
                             conn,
                             std::strerror(errno));
                throw std::runtime_error("TCP error receiving!");
            }

            // TODO: ??
            SPDLOG_ERROR("not expected: {}", std::strerror(errno));
        } else {
            buffer += got;
            numRecvd += got;
        }
    }

    assert(numRecvd == bufferSize);
}
}
