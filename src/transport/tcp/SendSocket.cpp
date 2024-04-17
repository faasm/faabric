#include <faabric/transport/tcp/SendSocket.h>
#include <faabric/transport/tcp/SocketOptions.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>

#include <arpa/inet.h>
#ifdef FAABRIC_USE_SPINLOCK
#include <emmintrin.h>
#endif
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>

namespace faabric::transport::tcp {
SendSocket::SendSocket(const std::string& host, int port)
  : addr(host, port)
  , connected(false)
  , host(host)
  , port(port)
{}

void SendSocket::setSocketOptions(int connFd)
{
    setSendBufferSize(connFd, SocketBufferSizeBytes);
    setNoDelay(connFd);
    setQuickAck(connFd);

#ifdef FAABRIC_USE_SPINLOCK
    setNonBlocking(connFd);
#else
    // If we decide to make send sockets non-blocking, then in the sendOne loop
    // we need to treat the special case where we return EAGAIN or EWOULDBLOCK
    setBlocking(connFd);
    setSendTimeoutMs(connFd, SocketTimeoutMs);
#endif
}

void SendSocket::dial()
{
    if (connected) {
        return;
    }

    int connFd = sock.get();
    setSocketOptions(connFd);

    // Re-dial a number of times to accoun for races during initialisation.
    // This number must be rather high for higher-latency environments with
    // high core count
    int numRetries = 30;
    int pollPeriodMs = 200;
    int numTry = 0;

    int ret = ::connect(connFd, addr.get(), sizeof(sockaddr_in));
    while ((ret != 0) && (numTry < numRetries)) {
        numTry++;
        SPDLOG_TRACE("Retrying connection to {}:{} (attempt: {}/{}, ret: {})",
                     host,
                     port,
                     numTry,
                     numRetries,
                     ret);
        SLEEP_MS(pollPeriodMs);
        ret = ::connect(connFd, addr.get(), sizeof(sockaddr_in));
    }

    if (ret != 0) {
        SPDLOG_ERROR("Error connecting to {}:{}: {} (ret: {})",
                     host,
                     port,
                     std::strerror(errno),
                     ret);
        throw std::runtime_error(
          "TCP SendSocket error connecting to remote address");
    }

    SPDLOG_TRACE("TCP client connected to {}:{}", port, host);
    connected = true;
}

void SendSocket::sendOne(const uint8_t* buffer, size_t bufferSize)
{
    size_t totalNumSent = 0;

    while (totalNumSent < bufferSize) {
        size_t nSent = ::send(sock.get(), buffer, bufferSize - totalNumSent, 0);
        if (nSent == -1) {
#ifdef FAABRIC_USE_SPINLOCK
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                _mm_pause();
                continue;
            };
#endif
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                SPDLOG_ERROR(
                  "Error sending TCP message to {}:{} ({}/{}: timed-out)",
                  host,
                  port,
                  totalNumSent,
                  bufferSize);
            } else {
                SPDLOG_ERROR("Error sending TCP message to {}:{} ({}/{}): {}",
                             host,
                             port,
                             totalNumSent,
                             bufferSize,
                             std::strerror(errno));
            }

            throw std::runtime_error("Error sending TCP message!");
        }

        buffer += nSent;
        totalNumSent += nSent;
    }

    assert(totalNumSent == bufferSize);
}
}
