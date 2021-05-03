#pragma once

#include <faabric/transport/MessageEndpoint.h>

namespace faabric::transport {
/* Simple message endpoint implementation
 *
 * Low-level and simple message endpoint to run outstanding connections together
 * with higher-level client/server pairs.
 */
class SimpleMessageEndpoint : public faabric::transport::MessageEndpoint
{
  public:
    SimpleMessageEndpoint(const std::string& host, int port);

    ~SimpleMessageEndpoint();

    void recv(char*& msgData, int& msgSize);

  private:
    char* msgData;
    int msgSize;

    void doRecv(void* bodyData, int bodySize);
};
}
