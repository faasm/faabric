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

#define TEST_PORT_ASYNC 9998
#define TEST_PORT_SYNC 9999

class DummyServer final : public MessageEndpointServer
{
  public:
    DummyServer()
      : MessageEndpointServer(TEST_PORT_ASYNC, TEST_PORT_SYNC)
    {}

    std::atomic<int> messageCount = 0;

  private:
    void doAsyncRecv(int header,
                     const uint8_t* buffer,
                     size_t bufferSize) override
    {
        messageCount++;
    }

    std::unique_ptr<google::protobuf::Message>
    doSyncRecv(int header, const uint8_t* buffer, size_t bufferSize) override
    {
        messageCount++;

        return std::make_unique<faabric::EmptyResponse>();
    }
};

class EchoServer final : public MessageEndpointServer
{
  public:
    EchoServer()
      : MessageEndpointServer(TEST_PORT_ASYNC, TEST_PORT_SYNC)
    {}

  protected:
    void doAsyncRecv(int header,
                     const uint8_t* buffer,
                     size_t bufferSize) override
    {
        throw std::runtime_error("Echo server not expecting async recv");
    }

    std::unique_ptr<google::protobuf::Message>
    doSyncRecv(int header, const uint8_t* buffer, size_t bufferSize) override
    {
        SPDLOG_TRACE("Echo server received {} bytes", bufferSize);

        auto response = std::make_unique<faabric::StatePart>();
        response->set_data(buffer, bufferSize);

        return response;
    }
};

class SleepServer final : public MessageEndpointServer
{
  public:
    int delayMs = 1000;

    SleepServer()
      : MessageEndpointServer(TEST_PORT_ASYNC, TEST_PORT_SYNC)
    {}

  protected:
    void doAsyncRecv(int header,
                     const uint8_t* buffer,
                     size_t bufferSize) override
    {
        throw std::runtime_error("Sleep server not expecting async recv");
    }

    std::unique_ptr<google::protobuf::Message>
    doSyncRecv(int header, const uint8_t* buffer, size_t bufferSize) override
    {
        int* sleepTimeMs = (int*)buffer;
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

    MessageEndpointClient cli(LOCALHOST, TEST_PORT_ASYNC, TEST_PORT_SYNC);

    // Send a message
    std::string body = "body";
    const uint8_t* bodyMsg = BYTES_CONST(body.c_str());

    server.setAsyncLatch();
    cli.asyncSend(0, bodyMsg, body.size());
    server.awaitAsyncLatch();

    REQUIRE(server.messageCount == 1);

    server.stop();
}

TEST_CASE("Test send response to client", "[transport]")
{
    EchoServer server;
    server.start();

    std::string expectedMsg = "Response from server";

    // Open the source endpoint client
    MessageEndpointClient cli(LOCALHOST, TEST_PORT_ASYNC, TEST_PORT_SYNC);

    // Send and await the response
    faabric::StatePart response;
    cli.syncSend(0, BYTES(expectedMsg.data()), expectedMsg.size(), &response);

    assert(response.data() == expectedMsg);

    server.stop();
}

TEST_CASE("Test multiple clients talking to one server", "[transport]")
{
    EchoServer server;
    server.start();

    std::vector<std::thread> clientThreads;
    int numClients = 10;
    int numMessages = 1000;

    for (int i = 0; i < numClients; i++) {
        clientThreads.emplace_back(std::thread([i, numMessages] {
            // Prepare client
            MessageEndpointClient cli(
              LOCALHOST, TEST_PORT_ASYNC, TEST_PORT_SYNC);

            for (int j = 0; j < numMessages; j++) {
                std::string clientMsg =
                  fmt::format("Message {} from client {}", j, i);

                // Send and get response
                const uint8_t* body = BYTES_CONST(clientMsg.c_str());
                faabric::StatePart response;
                cli.syncSend(0, body, clientMsg.size(), &response);

                std::string actual = response.data();
                assert(actual == clientMsg);
            }
        }));
    }

    for (auto& t : clientThreads) {
        if (t.joinable()) {
            t.join();
        }
    }

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
      LOCALHOST, TEST_PORT_ASYNC, TEST_PORT_SYNC, clientTimeout);

    uint8_t* sleepBytes = BYTES(&serverSleep);
    faabric::StatePart response;

    if (expectFailure) {
        // Check for failure
        REQUIRE_THROWS_AS(cli.syncSend(0, sleepBytes, sizeof(int), &response),
                          MessageTimeoutException);
    } else {
        cli.syncSend(0, sleepBytes, sizeof(int), &response);
        REQUIRE(response.data() == "Response after sleep");
    }

    server.stop();
}
}
