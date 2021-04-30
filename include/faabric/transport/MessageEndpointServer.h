#pragma once

#include <faabric/transport/MessageContext.h>
#include <faabric/transport/MessageEndpoint.h>

#include <thread>

namespace faabric::transport {
/* Message endpoint with server-like behaviour
 *
 * This abstract class implements a server-like loop functionality and will
 * always run in the background.
 */
class MessageEndpointServer : public faabric::transport::MessageEndpoint
{
  public:
    MessageEndpointServer(const std::string& host, int port);

    void start(faabric::transport::MessageContext& context);

    void stop(faabric::transport::MessageContext& context);

    void recv();

    // Provide another template to receive messages with header and body
    virtual void doRecv(const void* headerBody,
                        int headerSize,
                        const void* bodyData,
                        int bodySize) = 0;

  private:
    std::thread servingThread;
};
}
