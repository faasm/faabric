#include <catch.hpp>

#include <thread>
#include <unistd.h>

#include <faabric/transport/MessageEndpointClient.h>

using namespace faabric::transport;

const std::string thisHost = "127.0.0.1";
const int testPort = 9999;
const int testReplyPort = 9996;

namespace tests {
class MessageContextFixture
{
  protected:
    MessageContext& context;

  public:
    MessageContextFixture()
      : context(getGlobalMessageContext())
    {}

    ~MessageContextFixture() { context.close(); }
};

TEST_CASE_METHOD(MessageContextFixture,
                 "Test open/close one client",
                 "[transport]")
{
    // Open an endpoint client, don't bind
    MessageEndpoint cli(thisHost, testPort);
    REQUIRE_NOTHROW(cli.open(context, SocketType::PULL, false));

    // Open another endpoint client, bind
    MessageEndpoint secondCli(thisHost, testPort);
    REQUIRE_NOTHROW(secondCli.open(context, SocketType::PUSH, true));

    // Close all endpoint clients
    REQUIRE_NOTHROW(cli.close(false));
    REQUIRE_NOTHROW(secondCli.close(true));
}

TEST_CASE_METHOD(MessageContextFixture,
                 "Test send/recv one message",
                 "[transport]")
{
    // Open the source endpoint client, don't bind
    SendMessageEndpoint src(thisHost, testPort);
    src.open(context);

    // Open the destination endpoint client, bind
    RecvMessageEndpoint dst(testPort);
    dst.open(context);

    // Send message
    std::string expectedMsg = "Hello world!";
    uint8_t msg[expectedMsg.size()];
    memcpy(msg, expectedMsg.c_str(), expectedMsg.size());
    REQUIRE_NOTHROW(src.send(msg, expectedMsg.size()));

    // Receive message
    Message recvMsg = dst.recv();
    REQUIRE(recvMsg.size() == expectedMsg.size());
    std::string actualMsg(recvMsg.data(), recvMsg.size());
    REQUIRE(actualMsg == expectedMsg);

    // Close endpoints
    src.close();
    dst.close();
}

TEST_CASE_METHOD(MessageContextFixture, "Test await response", "[transport]")
{
    // Prepare common message/response
    std::string expectedMsg = "Hello ";
    std::string expectedResponse = "world!";

    std::thread senderThread([this, expectedMsg, expectedResponse] {
        // Open the source endpoint client, don't bind
        MessageEndpointClient src(thisHost, testPort);
        src.open(context);

        // Send message and wait for response
        uint8_t msg[expectedMsg.size()];
        memcpy(msg, expectedMsg.c_str(), expectedMsg.size());
        src.send(msg, expectedMsg.size());

        // Block waiting for a response
        Message recvMsg = src.awaitResponse(testReplyPort);
        assert(recvMsg.size() == expectedResponse.size());
        std::string actualResponse(recvMsg.data(), recvMsg.size());
        assert(actualResponse == expectedResponse);

        src.close();
    });

    // Receive message
    RecvMessageEndpoint dst(testPort);
    dst.open(context);
    Message recvMsg = dst.recv();
    REQUIRE(recvMsg.size() == expectedMsg.size());
    std::string actualMsg(recvMsg.data(), recvMsg.size());
    REQUIRE(actualMsg == expectedMsg);

    // Send response, open a new endpoint for it
    SendMessageEndpoint dstResponse(thisHost, testReplyPort);
    dstResponse.open(context);
    uint8_t msg[expectedResponse.size()];
    memcpy(msg, expectedResponse.c_str(), expectedResponse.size());
    dstResponse.send(msg, expectedResponse.size());

    // Wait for sender thread
    if (senderThread.joinable()) {
        senderThread.join();
    }

    // Close receiving endpoints
    dst.close();
    dstResponse.close();
}

TEST_CASE_METHOD(MessageContextFixture,
                 "Test send/recv many messages",
                 "[transport]")
{
    int numMessages = 10000;
    std::string baseMsg = "Hello ";

    std::thread senderThread([this, numMessages, baseMsg] {
        // Open the source endpoint client, don't bind
        SendMessageEndpoint src(thisHost, testPort);
        src.open(context);
        for (int i = 0; i < numMessages; i++) {
            std::string expectedMsg = baseMsg + std::to_string(i);
            uint8_t msg[expectedMsg.size()];
            memcpy(msg, expectedMsg.c_str(), expectedMsg.size());
            src.send(msg, expectedMsg.size());
        }

        src.close();
    });

    // Receive messages
    RecvMessageEndpoint dst(testPort);
    dst.open(context);
    for (int i = 0; i < numMessages; i++) {
        Message recvMsg = dst.recv();
        // Check just a subset of the messages
        // Note - this implicitly tests in-order message delivery
        if ((i % (numMessages / 10)) == 0) {
            std::string expectedMsg = baseMsg + std::to_string(i);
            REQUIRE(recvMsg.size() == expectedMsg.size());
            std::string actualMsg(recvMsg.data(), recvMsg.size());
            REQUIRE(actualMsg == expectedMsg);
        }
    }

    // Wait for the sender thread to finish
    if (senderThread.joinable()) {
        senderThread.join();
    }

    // Close the destination endpoint
    dst.close();
}

TEST_CASE_METHOD(MessageContextFixture,
                 "Test send/recv many messages from many clients",
                 "[transport]")
{
    int numMessages = 10000;
    int numSenders = 10;
    std::string expectedMsg = "Hello from client";
    std::vector<std::thread> senderThreads;

    for (int j = 0; j < numSenders; j++) {
        senderThreads.emplace_back(
          std::thread([this, numMessages, expectedMsg] {
              // Open the source endpoint client, don't bind
              SendMessageEndpoint src(thisHost, testPort);
              src.open(context);
              for (int i = 0; i < numMessages; i++) {
                  uint8_t msg[expectedMsg.size()];
                  memcpy(msg, expectedMsg.c_str(), expectedMsg.size());
                  src.send(msg, expectedMsg.size());
              }

              src.close();
          }));
    }

    // Receive messages
    RecvMessageEndpoint dst(testPort);
    dst.open(context);
    for (int i = 0; i < numSenders * numMessages; i++) {
        Message recvMsg = dst.recv();
        // Check just a subset of the messages
        if ((i % numMessages) == 0) {
            REQUIRE(recvMsg.size() == expectedMsg.size());
            std::string actualMsg(recvMsg.data(), recvMsg.size());
            REQUIRE(actualMsg == expectedMsg);
        }
    }

    // Wait for the sender thread to finish
    for (auto& t : senderThreads) {
        if (t.joinable()) {
            t.join();
        }
    }

    // Close the destination endpoint
    dst.close();
}
}
