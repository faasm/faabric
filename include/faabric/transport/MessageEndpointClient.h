#pragma once

#include <faabric/transport/MessageEndpoint.h>
#include <faabric/transport/common.h>

namespace faabric::transport {
/* Minimal message endpoint client
 *
 * Low-level and minimal message endpoint client to run in companion with
 * a background-running server.
 */
class MessageEndpointClient : public faabric::transport::MessageEndpoint
{
  public:
    MessageEndpointClient(const std::string& host, int port);

    /* Wait for a message
     *
     * This method blocks the calling thread until we receive a message from
     * the specified host:port pair. When pointed at a server, this method
     * allows for blocking communications.
     */
    Message awaitResponse(const std::string& host, int port);
};
}
