#include <catch.hpp>

#include "faabric_utils.h"

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

class SleepServer final : public MessageEndpointServer
{
  public:
    int delayMs = 1000;

    SleepServer()
      : MessageEndpointServer(testPortAsync, testPortSync)
    {}

  protected:
    void doAsyncRecv(faabric::transport::Message& header,
                     faabric::transport::Message& body) override
    {
        throw std::runtime_error("Sleep server not expecting async recv");
    }

    std::unique_ptr<google::protobuf::Message> doSyncRecv(
      faabric::transport::Message& header,
      faabric::transport::Message& body) override
    {
        int* sleepTimeMs = (int*)body.udata();
        SPDLOG_DEBUG("Sleep server sleeping for {}ms", *sleepTimeMs);
        SLEEP_MS(*sleepTimeMs);

        auto response = std::make_unique<faabric::StatePart>();
        response->set_data("Response after sleep");
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

    REQUIRE_RETRY({}, server.messageCount == 1);

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
    // Start the server in the background
    DummyServer server;
    server.start();

    std::vector<std::thread> clientThreads;
    int numClients = 10;
    int numMessages = 1000;

    // Set up a barrier to wait on all the clients having finished
    faabric::util::Barrier barrier(numClients + 1);

    for (int i = 0; i < numClients; i++) {
        clientThreads.emplace_back(std::thread([&barrier, numMessages] {
            // Prepare client
            MessageEndpointClient cli(thisHost, testPortAsync, testPortSync);

            std::string clientMsg = "Message from threaded client";
            for (int j = 0; j < numMessages; j++) {
                // Send body
                uint8_t body[clientMsg.size()];
                memcpy(body, clientMsg.c_str(), clientMsg.size());
                cli.asyncSend(0, body, clientMsg.size());
            }

            barrier.wait();
        }));
    }

    barrier.wait();

    for (auto& t : clientThreads) {
        if (t.joinable()) {
            t.join();
        }
    }

    REQUIRE_RETRY({}, server.messageCount == numMessages * numClients);

    server.stop();
}

TEST_CASE("Test client timeout on requests to valid server", "[transport]")
{
    int clientTimeout;
    int serverSleep;
    bool expectFailure;

    SECTION("Long timeout no failure")
    {
        clientTimeout = 20000;
        serverSleep = 100;
        expectFailure = false;
    }

    SECTION("Short timeout failure")
    {
        clientTimeout = 10;
        serverSleep = 2000;
        expectFailure = true;
    }

    // Start the server
    SleepServer server;
    server.start();

    // Set up the client
    MessageEndpointClient cli(
      thisHost, testPortAsync, testPortSync, clientTimeout);

    uint8_t* sleepBytes = BYTES(&serverSleep);
    faabric::StatePart response;

    if (expectFailure) {
        // Check for failure
        REQUIRE_THROWS_AS(cli.syncSend(0, sleepBytes, sizeof(int), &response),
                          MessageTimeoutException);
    } else {
        cli.syncSend(0, sleepBytes, sizeof(int), &response);
        std::vector<uint8_t> expected = { 0, 1, 2, 3 };
        REQUIRE(response.data() == "Response after sleep");
    }

    server.stop();
}
}
