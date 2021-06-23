#include <catch.hpp>

#include <thread>

#include <faabric/transport/MessageEndpointServer.h>
#include <faabric/transport/common.h>
#include <faabric/util/logging.h>

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
        messageCount++;
    }
};

class SlowServer final : public MessageEndpointServer
{
  public:
    int delayMs = 1000;
    std::vector<uint8_t> data = { 0, 1, 2, 3 };

    SlowServer()
      : MessageEndpointServer(testPort)
    {}

  private:
    void doRecv(faabric::transport::Message& header,
                faabric::transport::Message& body) override
    {
        SPDLOG_DEBUG("Slow message server test recv");

        usleep(delayMs * 1000);
        MessageEndpointServer::sendResponse(
          data.data(), data.size(), thisHost, testPort);
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
    SendMessageEndpoint src(thisHost, testPort);

    // Send message: server expects header + body
    std::string header = "header";
    uint8_t headerMsg[header.size()];
    memcpy(headerMsg, header.c_str(), header.size());

    // Mark we are sending the header
    src.send(headerMsg, header.size(), true);

    // Send the body
    std::string body = "body";
    uint8_t bodyMsg[body.size()];
    memcpy(bodyMsg, body.c_str(), body.size());
    src.send(bodyMsg, body.size(), false);

    usleep(1000 * 300);
    REQUIRE(server.messageCount == 1);

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
        SendMessageEndpoint cli(thisHost, testPort);

        Message msg = cli.awaitResponse(testPort + REPLY_PORT_OFFSET);
        assert(msg.size() == expectedMsg.size());
        std::string actualMsg(msg.data(), msg.size());
        assert(actualMsg == expectedMsg);
    });

    uint8_t msg[expectedMsg.size()];
    memcpy(msg, expectedMsg.c_str(), expectedMsg.size());
    server.sendResponse(msg, expectedMsg.size(), thisHost, testPort);

    if (clientThread.joinable()) {
        clientThread.join();
    }

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
            SendMessageEndpoint cli(thisHost, testPort);

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
        }));
    }

    for (auto& t : clientThreads) {
        if (t.joinable()) {
            t.join();
        }
    }

    REQUIRE(server.messageCount == numMessages * numClients);

    server.stop();
}

TEST_CASE("Test client timeout on requests to valid server", "[transport]")
{
    int clientTimeout;
    bool expectFailure;

    SECTION("Long timeout no failure")
    {
        clientTimeout = 20000;
        expectFailure = false;
    }

    SECTION("Short timeout failure")
    {
        clientTimeout = 1;
        expectFailure = true;
    }

    // Start the server in the background
    std::thread t([] {
        SlowServer server;
        server.start();

        int threadSleep = server.delayMs + 500;
        usleep(threadSleep * 1000);

        server.stop();
    });

    // Wait for the server to start up
    usleep(500 * 1000);

    // Set up the client
    SendMessageEndpoint cli(thisHost, testPort, clientTimeout);

    std::vector<uint8_t> data = { 1, 1, 1 };
    cli.send(data.data(), data.size(), true);
    cli.send(data.data(), data.size());

    if (expectFailure) {
        // Check for failure
        REQUIRE_THROWS_AS(cli.awaitResponse(testPort + REPLY_PORT_OFFSET),
                          MessageTimeoutException);
    } else {
        // Check response from server successful
        Message responseMessage =
          cli.awaitResponse(testPort + REPLY_PORT_OFFSET);
        std::vector<uint8_t> expected = { 0, 1, 2, 3 };
        REQUIRE(responseMessage.dataCopy() == expected);
    }

    if (t.joinable()) {
        t.join();
    }
}
}
