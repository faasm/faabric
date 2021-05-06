#pragma once

#include <faabric/transport/MessageEndpoint.h>
#include <faabric/transport/common.h>

namespace faabric::transport {
/* Minimal message endpoint client
 *
 * Low-level and minimal message endpoint client to run in companion with
 * with higher-level client/server pairs.
 */
class MessageEndpointClient : public faabric::transport::MessageEndpoint
{
  public:
    MessageEndpointClient(const std::string& host, int port);

    ~MessageEndpointClient();

    void recv(char*& msgData, int& msgSize);

    /* Wait for a message
     *
     * This method blocks the calling thread until we receive a message from
     * the specified host:port pair. When pointed at a server, this method
     * allows for blocking communications.
     */
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
