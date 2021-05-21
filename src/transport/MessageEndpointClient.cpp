#include <faabric/transport/MessageEndpointClient.h>

namespace faabric::transport {
/*
class ClosedMonitor : public zmq::monitor_t {
public:
    void on_event_closed(const zmq_event_t& event, const char* addr) override
    {
        faabric::util::getLogger()->error("socket disconnected");
    }
};
*/

MessageEndpointClient::MessageEndpointClient(const std::string& host, int port)
  : SendMessageEndpoint(host, port)
{}

// Block until we receive a response from the server
Message MessageEndpointClient::awaitResponse(int port)
{
    // Wait for the response, open a temporary endpoint for it
    // Note - we use a different host/port not to clash with existing server
    RecvMessageEndpoint endpoint(port);
    endpoint.open(faabric::transport::getGlobalMessageContext());
    Message receivedMessage = endpoint.recv();
    endpoint.close();

    return receivedMessage;
}
}
