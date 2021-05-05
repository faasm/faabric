#pragma once

#include <faabric/transport/MessageContext.h>
#include <faabric/transport/MessageEndpoint.h>
#include <faabric/transport/SimpleMessageEndpoint.h>

#include <thread>

namespace faabric::transport {
/* Server handling a long-running 0MQ socket
 *
 * This abstract class implements a server-like loop functionality and will
 * always run in the background. Note that message endpoints (i.e. 0MQ sockets)
 * are _not_ thread safe, must be open-ed and close-ed from the _same_ thread,
 * and thus should preferably live in the thread's local address space.
 */
class MessageEndpointServer
{
  public:
    MessageEndpointServer(const std::string& hostIn, int portIn);

    void start(faabric::transport::MessageContext& context);

    void stop(faabric::transport::MessageContext& context);

    void recv(faabric::transport::SimpleMessageEndpoint& endpoint);

    // Provide another template to receive messages with header and body
    virtual void doRecv(const void* headerBody,
                        int headerSize,
                        const void* bodyData,
                        int bodySize) = 0;

  private:
    const std::string host;
    const int port;

    std::thread servingThread;
};
}
