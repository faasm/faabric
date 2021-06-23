#pragma once

#include <faabric/transport/Message.h>
#include <faabric/transport/MessageEndpoint.h>

#include <thread>

#define ENDPOINT_SERVER_SHUTDOWN -1

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

    void start();

    virtual void stop();

  protected:
    bool recv();

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

    std::unique_ptr<RecvMessageEndpoint> recvEndpoint = nullptr;

    std::thread servingThread;
};
}
