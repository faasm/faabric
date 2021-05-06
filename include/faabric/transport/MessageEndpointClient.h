#pragma once

#include <faabric/transport/MessageEndpoint.h>
#include <faabric/transport/common.h>

namespace faabric::transport {
/* Simple message endpoint implementation
 *
 * Low-level and simple message endpoint to run outstanding connections together
 * with higher-level client/server pairs.
 */
class MessageEndpointClient : public faabric::transport::MessageEndpoint
{
  public:
    MessageEndpointClient(const std::string& host, int port);

    ~MessageEndpointClient();

    void recv(char*& msgData, int& msgSize);

    void awaitResponse(const std::string& host,
                       int port,
                       char*& data,
                       int& size);

  private:
    char* msgData;
    int msgSize;

    void doRecv(void* bodyData, int bodySize);
};
}
