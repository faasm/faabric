#include "faabric/proto/faabric.pb.h"
#include <catch.hpp>

#include <thread>

#include <faabric/transport/MessageEndpointServer.h>
#include <faabric/transport/common.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>

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

  private:
    void doAsyncRecv(faabric::transport::Message& header,
                     faabric::transport::Message& body) override
    {
        messageCount++;
    }

    std::unique_ptr<google::protobuf::Message> doSyncRecv(
      faabric::transport::Message& header,
      faabric::transport::Message& body) override
    {
        messageCount++;

        return std::make_unique<faabric::EmptyResponse>();
    }
};

class EchoServer final : public MessageEndpointServer
{
  public:
    EchoServer()
      : MessageEndpointServer(testPort)
    {}

  protected:
    void doAsyncRecv(faabric::transport::Message& header,
                     faabric::transport::Message& body) override
    {
        throw std::runtime_error("EchoServer not expecting async recv");
    }

    std::unique_ptr<google::protobuf::Message> doSyncRecv(
      faabric::transport::Message& header,
      faabric::transport::Message& body) override
    {
        SPDLOG_TRACE("Echo server received {} bytes", body.size());

        auto response = std::make_unique<faabric::StatePart>();
        response->set_data(body.data(), body.size());

        return response;
    }
};

class SlowServer final : public MessageEndpointServer
{
  public:
    int delayMs = 1000;

    SlowServer()
      : MessageEndpointServer(testPort)
    {}

  protected:
    void doAsyncRecv(faabric::transport::Message& header,
                     faabric::transport::Message& body) override
    {
        throw std::runtime_error("SlowServer not expecting async recv");
    }

    std::unique_ptr<google::protobuf::Message> doSyncRecv(
      faabric::transport::Message& header,
      faabric::transport::Message& body) override
    {
        SPDLOG_DEBUG("Slow message server test recv");

        usleep(delayMs * 1000);
        auto response = std::make_unique<faabric::StatePart>();
        response->set_data("From the slow server");
        return response;
    }
};

namespace tests {
TEST_CASE("Test start/stop server", "[transport]")
{
    DummyServer server;
    server.start();

    usleep(100 * 1000);

    server.stop();
}

TEST_CASE("Test send one message to server", "[transport]")
{
    // Start server
    DummyServer server;
    server.start();

    // Open the source endpoint client, don't bind
    AsyncSendMessageEndpoint src(thisHost, testPort);

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

TEST_CASE("Test send response to client", "[transport]")
{
    std::thread serverThread([] {
        EchoServer server;
        server.start();
        usleep(1000 * 1000);
        server.stop();
    });

    std::string expectedMsg = "Response from server";

    // Open the source endpoint client, don't bind
    SyncSendMessageEndpoint cli(thisHost, testPort + 1);

    // Send and await the response
    cli.sendHeader(1);
    Message responseMsg =
      cli.sendAwaitResponse(BYTES(expectedMsg.data()), expectedMsg.size());

    faabric::StatePart response;
    response.ParseFromArray(responseMsg.data(), responseMsg.size());

    assert(response.data() == expectedMsg);

    if (serverThread.joinable()) {
        serverThread.join();
    }
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
            AsyncSendMessageEndpoint cli(thisHost, testPort);

            std::string clientMsg = "Message from threaded client";
            for (int j = 0; j < numMessages; j++) {
                // Send header
                cli.sendHeader(1);

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
        clientTimeout = 10;
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
    SyncSendMessageEndpoint cli(thisHost, testPort + 1, clientTimeout);

    std::vector<uint8_t> data = { 1, 1, 1 };
    cli.sendHeader(1);

    if (expectFailure) {
        // Check for failure
        REQUIRE_THROWS_AS(cli.sendAwaitResponse(data.data(), data.size()),
                          MessageTimeoutException);
    } else {
        Message responseMsg = cli.sendAwaitResponse(data.data(), data.size());
        faabric::StatePart response;
        response.ParseFromArray(responseMsg.data(), responseMsg.size());

        std::vector<uint8_t> expected = { 0, 1, 2, 3 };
        REQUIRE(response.data() == "From the slow server");
    }

    if (t.joinable()) {
        t.join();
    }
}
}
