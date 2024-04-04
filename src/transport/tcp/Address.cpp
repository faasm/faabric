#include <faabric/transport/common.h>
#include <faabric/transport/tcp/Address.h>

#include <arpa/inet.h>
#include <cstring>
#include <sys/socket.h>

namespace faabric::transport::tcp {
Address::Address(const std::string& host, int port)
{
    std::memset(&addr, 0, sizeof(sockaddr_in));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (host != ANY_HOST) {
        inet_pton(AF_INET, host.c_str(), &addr.sin_addr);
    }
}

Address::Address(int port)
  : Address(ANY_HOST, port){};

sockaddr* Address::get() const
{
    return (sockaddr*)&addr;
}
}
