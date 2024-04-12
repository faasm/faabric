#include <faabric/transport/tcp/RecvSocket.h>
#include <faabric/transport/tcp/SocketOptions.h>
#include <faabric/util/logging.h>

#include <emmintrin.h>
#include <poll.h>

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
    setReuseAddr(connFd);
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

    // We cannot set a timeout on the ACCEPT system call. Instead, we poll
    // on the listening file descriptor until someone has CONNECT-ed to us
    // tiggering a POLLIN event
    struct pollfd polledFds[1];
    polledFds[0].fd = connFd;
    polledFds[0].events = POLLIN;
    int pollTimeoutMs = 2000;
    int numReady = ::poll(polledFds, 1, pollTimeoutMs);
    if (numReady < 1) {
        SPDLOG_ERROR(
          "Error accepting connection on {}:{} (fd: {}): poll timed out",
          host,
          port,
          connFd);
        throw std::runtime_error("Poll timed-out!");
    }

    // Once poll has returned succesfully, we should be able to accept
    int newConn = ::accept(sock.get(), 0, 0);
    if (newConn < 1) {
        SPDLOG_ERROR("Error accepting connection on {}:{} (fd: {}): {}",
                     host,
                     port,
                     connFd,
                     std::strerror(errno));
        throw std::runtime_error("Error accepting TCP connection");
    }

    // Set socket options for the newly created receive socket
    setSocketOptions(newConn);

    // TODO: add constructor parameter of max num conn
    openConnections.push_back(newConn);

    return newConn;
}

// Single function to configure _all_ TCP options for a reception socket
void RecvSocket::setSocketOptions(int connFd)
{
#ifdef FAABRIC_USE_SPINLOCK
    if (!isNonBlocking(connFd)) {
        setNonBlocking(connFd);
    }

    // TODO: not clear if this helps or not
    setBusyPolling(connFd);
#else
    // Set the socket as blocking
    if (isNonBlocking(connFd)) {
        setBlocking(connFd);
    }

    // Set the timeout
    setTimeoutMs(connFd, SocketTimeoutMs);
#endif
}

void RecvSocket::recvOne(int conn, uint8_t* buffer, size_t bufferSize)
{
    size_t numRecvd = 0;

    while (numRecvd < bufferSize) {
        // Receive from socket
#ifdef FAABRIC_USE_SPINLOCK
        int got = ::recv(conn, buffer, bufferSize - numRecvd, MSG_DONTWAIT);
#else
        int got = ::recv(conn, buffer, bufferSize - numRecvd, 0);
#endif
        if (got == -1) {
#ifdef FAABRIC_USE_SPINLOCK
            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
                _mm_pause();
#else
            if (errno == EAGAIN) {
#endif
                continue;
            }

            SPDLOG_ERROR("TCP Server error receiving in {}: {}",
                         conn,
                         std::strerror(errno));
            throw std::runtime_error("TCP error receiving!");
        }

        buffer += got;
        numRecvd += got;
    }

    assert(numRecvd == bufferSize);
}
}
