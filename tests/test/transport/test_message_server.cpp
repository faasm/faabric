#include <catch2/catch.hpp>

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
      : MessageEndpointServer(TEST_PORT_ASYNC, TEST_PORT_SYNC, "test-dummy", 2)
    {}

    std::atomic<int> messageCount = 0;

  private:
    void doAsyncRecv(transport::Message&& message) override { messageCount++; }

    std::unique_ptr<google::protobuf::Message> doSyncRecv(
      transport::Message&& message) override
    {
        messageCount++;

        return std::make_unique<faabric::EmptyResponse>();
    }
};

class EchoServer final : public MessageEndpointServer
{
  public:
    EchoServer()
      : MessageEndpointServer(TEST_PORT_ASYNC, TEST_PORT_SYNC, "test-echo", 2)
    {}

  protected:
    void doAsyncRecv(transport::Message&& message) override
    {
        throw std::runtime_error("Echo server not expecting async recv");
    }

    std::unique_ptr<google::protobuf::Message> doSyncRecv(
      transport::Message&& message) override
    {
        SPDLOG_TRACE("Echo server received {} bytes", message.size());

        auto response = std::make_unique<faabric::StatePart>();
        response->set_data(message.udata(), message.size());

        return response;
    }
};

class SleepServer final : public MessageEndpointServer
{
  public:
    int delayMs = 1000;

    SleepServer()
      : MessageEndpointServer(TEST_PORT_ASYNC, TEST_PORT_SYNC, "test-sleep", 2)
    {}

  protected:
    void doAsyncRecv(transport::Message&& message) override
    {
        throw std::runtime_error("Sleep server not expecting async recv");
    }

    std::unique_ptr<google::protobuf::Message> doSyncRecv(
      transport::Message&& message) override
    {
        int* sleepTimeMs = (int*)message.udata();
        SPDLOG_DEBUG("Sleep server sleeping for {}ms", *sleepTimeMs);
        SLEEP_MS(*sleepTimeMs);

        auto response = std::make_unique<faabric::StatePart>();
        response->set_data("Response after sleep");
        return response;
    }
};

class BlockServer final : public MessageEndpointServer
{
  public:
    BlockServer()
      : MessageEndpointServer(TEST_PORT_ASYNC, TEST_PORT_SYNC, "test-lock", 2)
      , latch(faabric::util::Latch::create(2))
    {}

  protected:
    void doAsyncRecv(transport::Message&& message) override
    {
        throw std::runtime_error("Lock server not expecting async recv");
    }

    std::unique_ptr<google::protobuf::Message> doSyncRecv(
      transport::Message&& message) override
    {
        // Wait on the latch, requires multiple threads executing in parallel to
        // get a response.
        latch->wait();

        // Echo input data
        auto response = std::make_unique<faabric::StatePart>();
        response->set_data(message.udata(), message.size());
        return response;
    }

  private:
    std::shared_ptr<faabric::util::Latch> latch = nullptr;
};

namespace tests {

TEST_CASE("Test sending one message to server", "[transport]")
{
    DummyServer server;
    server.start();

    SPDLOG_DEBUG("Dummy server started");

    REQUIRE(server.messageCount == 0);

    MessageEndpointClient cli(LOCALHOST, TEST_PORT_ASYNC, TEST_PORT_SYNC);

    // Send a message
    std::string body = "body";
    const uint8_t* bodyMsg = BYTES_CONST(body.c_str());

    server.setRequestLatch();
    cli.asyncSend(0, bodyMsg, body.size());
    server.awaitRequestLatch();

    REQUIRE(server.messageCount == 1);

    server.stop();
}

TEST_CASE("Test sending response to client", "[transport]")
{
    EchoServer server;
    server.start();

    std::string expectedMsg = "Response from server";

    // Open the source endpoint client
    MessageEndpointClient cli(LOCALHOST, TEST_PORT_ASYNC, TEST_PORT_SYNC);

    // Send and await the response
    faabric::StatePart response;
    cli.syncSend(0, BYTES(expectedMsg.data()), expectedMsg.size(), &response);

    REQUIRE(response.data() == expectedMsg);

    server.stop();
}

// This test hangs ThreadSanitizer
#if !(defined(__has_feature) && __has_feature(thread_sanitizer))
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
#endif

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
        bool failed = false;

        // Here we must wait until the server has finished handling the request,
        // even though it's failed
        server.setRequestLatch();

        // Make the call and check it fails
        try {
            cli.syncSend(0, sleepBytes, sizeof(int), &response);
        } catch (MessageTimeoutException& ex) {
            failed = true;
        }

        REQUIRE(failed);

        // Wait for request to finish
        server.awaitRequestLatch();
    } else {
        cli.syncSend(0, sleepBytes, sizeof(int), &response);
        REQUIRE(response.data() == "Response after sleep");
    }

    server.stop();
}

TEST_CASE("Test blocking requests in multi-threaded server", "[transport]")
{
    // Start server in the background
    BlockServer server;
    server.start();

    bool successes[2] = { false, false };

    // Create two background threads to make the blocking requests
    std::thread tA([&successes] {
        MessageEndpointClient cli(LOCALHOST, TEST_PORT_ASYNC, TEST_PORT_SYNC);

        std::string expectedMsg = "Background thread A";

        faabric::StatePart response;
        cli.syncSend(
          0, BYTES(expectedMsg.data()), expectedMsg.size(), &response);

        if (response.data() != expectedMsg) {
            SPDLOG_ERROR("A did not get expected response: {} != {}",
                         response.data(),
                         expectedMsg);
            successes[0] = false;
        } else {
            successes[0] = true;
        }
    });

    std::thread tB([&successes] {
        MessageEndpointClient cli(LOCALHOST, TEST_PORT_ASYNC, TEST_PORT_SYNC);

        std::string expectedMsg = "Background thread B";

        faabric::StatePart response;
        cli.syncSend(
          0, BYTES(expectedMsg.data()), expectedMsg.size(), &response);

        if (response.data() != expectedMsg) {
            SPDLOG_ERROR("B did not get expected response: {} != {}",
                         response.data(),
                         expectedMsg);

            successes[1] = false;
        } else {
            successes[1] = true;
        }
    });

    if (tA.joinable()) {
        tA.join();
    }

    if (tB.joinable()) {
        tB.join();
    }

    REQUIRE(successes[0]);
    REQUIRE(successes[1]);

    server.stop();
}
}
