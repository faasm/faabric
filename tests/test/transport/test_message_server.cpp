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
      : MessageEndpointServer(thisHost, testPort)
      , messageCount(0)
    {}

    // Variable to keep track of the received messages
    int messageCount;

    void sendResponse(char* serialisedMsg,
                      int size,
                      const std::string& returnHost,
                      int returnPort)
    {
        MessageEndpointServer::sendResponse(
          serialisedMsg, size, returnHost, returnPort);
    }

  private:
    void doRecv(const void* headerData,
                int headerSize,
                const void* bodyData,
                int bodySize) override
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
    src.open(context, SocketType::PUSH, false);

    // Send message: server expects header + body
    std::string header = "header";
    char* headerMsg = new char[header.size()]();
    memcpy(headerMsg, header.c_str(), header.size());
    // Mark we are sending the header
    src.send(headerMsg, header.size(), true);
    // Send the body
    std::string body = "body";
    char* bodyMsg = new char[body.size()]();
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
        cli.open(context, SocketType::PUSH, false);

        char* msg;
        int size;
        cli.awaitResponse(thisHost, testPort + REPLY_PORT_OFFSET, msg, size);
        REQUIRE(size == expectedMsg.size());
        std::string actualMsg(msg, size);
        REQUIRE(actualMsg == expectedMsg);

        cli.close();
    });

    char* msg = new char[expectedMsg.size()]();
    memcpy(msg, expectedMsg.c_str(), expectedMsg.size());
    REQUIRE_NOTHROW(
      server.sendResponse(msg, expectedMsg.size(), thisHost, testPort));

    clientThread.join();

    server.stop();
}
}
