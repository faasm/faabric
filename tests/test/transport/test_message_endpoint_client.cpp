#include <catch.hpp>

#include <thread>
#include <unistd.h>

#include <faabric/transport/MessageEndpointClient.h>

using namespace faabric::transport;

const std::string thisHost = "127.0.0.1";
const int testPort = 9999;
const int testReplyPort = 9996;

namespace tests {
TEST_CASE("Test open/close one client", "[transport]")
{
    // Get message context
    auto& context = getGlobalMessageContext();

    // Open an endpoint client, don't bind
    MessageEndpoint cli(thisHost, testPort);
    REQUIRE_NOTHROW(cli.open(context, SocketType::PULL, false));

    // Open another endpoint client, bind
    MessageEndpoint secondCli(thisHost, testPort);
    REQUIRE_NOTHROW(secondCli.open(context, SocketType::PUSH, true));

    // Open a third endpoint, bind as well. Should fail: can't bind two clients
    // to the same address
    MessageEndpoint thirdCli(thisHost, testPort);
    // REQUIRE_THROWS(thirdCli.open(context, SocketType::PUSH, true));

    // Close all endpoint clients
    REQUIRE_NOTHROW(cli.close(false));
    REQUIRE_NOTHROW(secondCli.close(true));
    // REQUIRE_THROWS(thirdCli.close(true));

    // Close message context
    context.close();
}

TEST_CASE("Test send/recv one message", "[transport]")
{
    // Get message context
    auto& context = getGlobalMessageContext();

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

    // Close message context
    context.close();
}

TEST_CASE("Test await response", "[transport]")
{
    // Get message context
    auto& context = getGlobalMessageContext();

    // Prepare common message/response
    std::string expectedMsg = "Hello ";
    std::string expectedResponse = "world!";

    std::thread senderThread([&context, expectedMsg, expectedResponse] {
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

    // Close message context
    context.close();
}

TEST_CASE("Test send/recv many messages", "[transport]")
{
    auto& context = getGlobalMessageContext();

    int numMessages = 10000;
    std::string baseMsg = "Hello ";

    std::thread senderThread([&context, numMessages, baseMsg] {
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

    // Close the messaging context
    context.close();
}

TEST_CASE("Test send/recv many messages from many clients", "[transport]")
{
    auto& context = getGlobalMessageContext();

    int numMessages = 10000;
    int numSenders = 10;
    std::string expectedMsg = "Hello from client";
    std::vector<std::thread> senderThreads;

    for (int j = 0; j < numSenders; j++) {
        senderThreads.emplace_back(
          std::thread([&context, numMessages, expectedMsg] {
              // Open the source endpoint client, don't bind
              SendMessageEndpoint src(thisHost, testPort);
              src.open(context);
              for (int i = 0; i < numMessages; i++) {
                  uint8_t msg[expectedMsg.size()];
                  memcpy(msg, expectedMsg.c_str(), expectedMsg.size());
                  src.send(msg, expectedMsg.size());
              }

              // Give the receiver time to ingest all messages. Otherwise,
              // closing the endpoint will remove all outstanding messages. This
              // is because we, by default, set the LINGER period to 0.
              // usleep(1000 * 200);

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

    // Close the messaging context
    context.close();
}
}
