#include <catch.hpp>

#include <thread>

#include <faabric/transport/MessageEndpointServer.h>

using namespace faabric::transport;

const std::string thisHost = "127.0.0.1";
const int testPort = 9999;

class DummyServer final : public MessageEndpointServer
{
  public:
    DummyServer()
      : MessageEndpointServer(testPort)
      , messageCount(0)
    {}

    // Variable to keep track of the received messages
    int messageCount;

    // This method is protected in the base class, as it's always called from
    // the doRecv implementation. To ease testing, we make it public with this
    // workaround.
    void sendResponse(uint8_t* serialisedMsg,
                      int size,
                      const std::string& returnHost,
                      int returnPort)
    {
        MessageEndpointServer::sendResponse(
          serialisedMsg, size, returnHost, returnPort);
    }

  private:
    void doRecv(faabric::transport::Message& header,
                faabric::transport::Message& body) override
    {
        // Dummy server, do nothing but increment the message count
        this->messageCount++;
    }
};

namespace tests {
TEST_CASE("Test start/stop server", "[transport]")
{
    DummyServer server;
    REQUIRE_NOTHROW(server.start());

    usleep(1000 * 100);

    REQUIRE_NOTHROW(server.stop());
}

TEST_CASE("Test send one message to server", "[transport]")
{
    // Start server
    DummyServer server;
    server.start();

    // Open the source endpoint client, don't bind
    auto& context = getGlobalMessageContext();
    MessageEndpointClient src(thisHost, testPort);
    src.open(context);

    // Send message: server expects header + body
    std::string header = "header";
    uint8_t headerMsg[header.size()];
    memcpy(headerMsg, header.c_str(), header.size());
    // Mark we are sending the header
    REQUIRE_NOTHROW(src.send(headerMsg, header.size(), true));
    // Send the body
    std::string body = "body";
    uint8_t bodyMsg[body.size()];
    memcpy(bodyMsg, body.c_str(), body.size());
    src.send(bodyMsg, body.size(), false);

    usleep(1000 * 300);
    REQUIRE(server.messageCount == 1);

    // Close the client
    src.close();

    // Close the server
    server.stop();
}

TEST_CASE("Test send one-off response to client", "[transport]")
{
    DummyServer server;
    server.start();

    std::string expectedMsg = "Response from server";

    std::thread clientThread([expectedMsg] {
        // Open the source endpoint client, don't bind
        auto& context = getGlobalMessageContext();
        MessageEndpointClient cli(thisHost, testPort);
        cli.open(context);

        Message msg = cli.awaitResponse(testPort + REPLY_PORT_OFFSET);
        assert(msg.size() == expectedMsg.size());
        std::string actualMsg(msg.data(), msg.size());
        assert(actualMsg == expectedMsg);

        cli.close();
    });

    uint8_t msg[expectedMsg.size()];
    memcpy(msg, expectedMsg.c_str(), expectedMsg.size());
    REQUIRE_NOTHROW(
      server.sendResponse(msg, expectedMsg.size(), thisHost, testPort));

    clientThread.join();

    server.stop();
}

TEST_CASE("Test multiple clients talking to one server", "[transport]")
{
    DummyServer server;
    server.start();

    std::vector<std::thread> clientThreads;
    int numClients = 10;
    int numMessages = 1000;

    for (int i = 0; i < numClients; i++) {
        clientThreads.emplace_back(std::thread([numMessages] {
            // Prepare client
            auto& context = getGlobalMessageContext();
            MessageEndpointClient cli(thisHost, testPort);
            cli.open(context);

            std::string clientMsg = "Message from threaded client";
            for (int j = 0; j < numMessages; j++) {
                // Send header
                uint8_t header[clientMsg.size()];
                memcpy(header, clientMsg.c_str(), clientMsg.size());
                cli.send(header, clientMsg.size(), true);
                // Send body
                uint8_t body[clientMsg.size()];
                memcpy(body, clientMsg.c_str(), clientMsg.size());
                cli.send(body, clientMsg.size());
            }

            usleep(1000 * 300);

            cli.close();
        }));
    }

    for (auto& t : clientThreads) {
        t.join();
    }

    REQUIRE(server.messageCount == numMessages * numClients);

    server.stop();
}
}
