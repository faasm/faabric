#pragma once

#include <faabric/transport/Message.h>
#include <faabric/transport/MessageContext.h>
#include <faabric/transport/MessageEndpoint.h>
#include <faabric/transport/MessageEndpointClient.h>

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
    MessageEndpointServer(int portIn);

    /* Start and stop the server
     *
     * Generic methods to start and stop a message endpoint server. They take
     * a, thread-safe, 0MQ context as an argument. The stop method will block
     * until _all_ sockets within the context have been closed. Sockets blocking
     * on a `recv` will be interrupted with ETERM upon context closure.
     */
    void start(faabric::transport::MessageContext& context);

    void stop(faabric::transport::MessageContext& context);

    /* Common start and stop entrypoint
     *
     * Call the generic methods with the default global message context.
     */
    void start();

    virtual void stop();

  protected:
    void recv(faabric::transport::RecvMessageEndpoint& endpoint);

    /* Template function to handle message reception
     *
     * A message endpoint server in faabric expects each communication to be
     * a multi-part 0MQ message. One message containing the header, and another
     * one with the body. Note that 0MQ _guarantees_ in-order delivery.
     */
    virtual void doRecv(faabric::transport::Message& header,
                        faabric::transport::Message& body) = 0;

    /* Send response to the client
     *
     * Send a one-off response to a client identified by host:port pair.
     * Together with a blocking recv at the client side, this
     * method can be used to achieve synchronous client-server communication.
     */
    void sendResponse(uint8_t* serialisedMsg,
                      int size,
                      const std::string& returnHost,
                      int returnPort);

  private:
    const int port;

    std::thread servingThread;
};
}
