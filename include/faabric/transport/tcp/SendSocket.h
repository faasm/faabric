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

    template<size_t N>
    void sendMany(std::array<uint8_t*, N>& bufferArray,
                  std::array<size_t, N>& sizeArray)
    {
        std::array<struct iovec, N> msgs;

        size_t totalSize = 0;
        for (int i = 0; i < N; i++) {
            msgs.at(i) = { .iov_base = bufferArray.at(i),
                           .iov_len = sizeArray.at(i) };
            totalSize += sizeArray.at(i);
        }

        auto nSent = ::writev(sock.get(), msgs.data(), N);
        if (nSent != totalSize) {
            SPDLOG_ERROR("Error sending many to {}:{} ({}/{}): {}",
                         host,
                         port,
                         nSent,
                         totalSize,
                         std::strerror(errno));
            throw std::runtime_error("TCP SendSocket sendMany error!");
        }
    }

  private:
    Address addr;
    Socket sock;
    bool connected;

    std::string host;
    int port;

    setSocketOptions(int connFd);
};
}
