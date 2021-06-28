#include <catch.hpp>

#include <thread>

#include <faabric/proto/faabric.pb.h>
#include <faabric/transport/MessageEndpointClient.h>
#include <faabric/transport/MessageEndpointServer.h>
#include <faabric/transport/common.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>

using namespace faabric::transport;

static const std::string thisHost = "127.0.0.1";
static const int testPortAsync = 9998;
static const int testPortSync = 9999;

class DummyServer final : public MessageEndpointServer
{
  public:
    DummyServer()
      : MessageEndpointServer(testPortAsync, testPortSync)
      , messageCount(0)
    {}

    // Variable to keep track of the received messages
    int messageCount;

    void start() override
    {
        // In a CI environment tests can be slow to tear down fully, so we want
        // to sleep and retry if the initial connection fails.
        try {
            MessageEndpointServer::start();
        } catch (zmq::error_t& ex) {
            SPDLOG_WARN("Error connecting dummy server, retrying after delay");

            SLEEP_MS(1000);
            MessageEndpointServer::start();
        }
    }

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
      : MessageEndpointServer(testPortAsync, testPortSync)
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
      : MessageEndpointServer(testPortAsync, testPortSync)
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

        SLEEP_MS(delayMs);
        auto response = std::make_unique<faabric::StatePart>();
        response->set_data("From the slow server");
        return response;
    }
};

namespace tests {

    TEST_CASE("Test send one message to server", "[transport]")
{
    DummyServer server;
    server.start();

    REQUIRE(server.messageCount == 0);

    MessageEndpointClient cli(thisHost, testPortAsync, testPortSync);

    // Send a message
    std::string body = "body";
    uint8_t bodyMsg[body.size()];
    memcpy(bodyMsg, body.c_str(), body.size());
    cli.asyncSend(0, bodyMsg, body.size());

    SLEEP_MS(500);

    REQUIRE(server.messageCount == 1);

    server.stop();
}

TEST_CASE("Test send response to client", "[transport]")
{
    EchoServer server;
    server.start();

    std::string expectedMsg = "Response from server";

    // Open the source endpoint client
    MessageEndpointClient cli(thisHost, testPortAsync, testPortSync);

    // Send and await the response
    faabric::StatePart response;
    cli.syncSend(0, BYTES(expectedMsg.data()), expectedMsg.size(), &response);

    assert(response.data() == expectedMsg);

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
            MessageEndpointClient cli(thisHost, testPortAsync, testPortSync);

            std::string clientMsg = "Message from threaded client";
            for (int j = 0; j < numMessages; j++) {
                // Send body
                uint8_t body[clientMsg.size()];
                memcpy(body, clientMsg.c_str(), clientMsg.size());
                cli.asyncSend(0, body, clientMsg.size());
            }
        }));
    }

    for (auto& t : clientThreads) {
        if (t.joinable()) {
            t.join();
        }
    }

    SLEEP_MS(2000);

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

    // Start the server
    SlowServer server;
    server.start();

    // Set up the client
    MessageEndpointClient cli(
      thisHost, testPortAsync, testPortSync, clientTimeout);
    std::vector<uint8_t> data = { 1, 1, 1 };
    faabric::StatePart response;

    if (expectFailure) {
        // Check for failure
        REQUIRE_THROWS_AS(cli.syncSend(0, data.data(), data.size(), &response),
                          MessageTimeoutException);
    } else {
        cli.syncSend(0, data.data(), data.size(), &response);

        std::vector<uint8_t> expected = { 0, 1, 2, 3 };
        REQUIRE(response.data() == "From the slow server");
    }

    server.stop();
}
}
