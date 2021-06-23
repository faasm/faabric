#include "faabric_utils.h"
#include <catch.hpp>

#include <thread>
#include <unistd.h>

#include <faabric/transport/MessageEndpointClient.h>

using namespace faabric::transport;

const std::string thisHost = "127.0.0.1";
const int testPort = 9999;
const int testReplyPort = 9996;

namespace tests {

TEST_CASE_METHOD(MessageContextFixture,
                 "Test send/recv one message",
                 "[transport]")
{
    // Open the source endpoint client, don't bind
    SendMessageEndpoint src(thisHost, testPort);

    // Open the destination endpoint client, bind
    RecvMessageEndpoint dst(testPort);

    // Send message
    std::string expectedMsg = "Hello world!";
    uint8_t msg[expectedMsg.size()];
    memcpy(msg, expectedMsg.c_str(), expectedMsg.size());
    REQUIRE_NOTHROW(src.send(msg, expectedMsg.size()));

    // Receive message
    faabric::transport::Message recvMsg = dst.recv();
    REQUIRE(recvMsg.size() == expectedMsg.size());
    std::string actualMsg(recvMsg.data(), recvMsg.size());
    REQUIRE(actualMsg == expectedMsg);
}

TEST_CASE_METHOD(MessageContextFixture, "Test await response", "[transport]")
{
    // Prepare common message/response
    std::string expectedMsg = "Hello ";
    std::string expectedResponse = "world!";

    std::thread senderThread([expectedMsg, expectedResponse] {
        // Open the source endpoint client, don't bind
        MessageEndpointClient src(thisHost, testPort);

        // Send message and wait for response
        uint8_t msg[expectedMsg.size()];
        memcpy(msg, expectedMsg.c_str(), expectedMsg.size());
        src.send(msg, expectedMsg.size());

        // Block waiting for a response
        faabric::transport::Message recvMsg = src.awaitResponse(testReplyPort);
        assert(recvMsg.size() == expectedResponse.size());
        std::string actualResponse(recvMsg.data(), recvMsg.size());
        assert(actualResponse == expectedResponse);
    });

    // Receive message
    RecvMessageEndpoint dst(testPort);
    faabric::transport::Message recvMsg = dst.recv();
    REQUIRE(recvMsg.size() == expectedMsg.size());
    std::string actualMsg(recvMsg.data(), recvMsg.size());
    REQUIRE(actualMsg == expectedMsg);

    // Send response, open a new endpoint for it
    SendMessageEndpoint dstResponse(thisHost, testReplyPort);
    uint8_t msg[expectedResponse.size()];
    memcpy(msg, expectedResponse.c_str(), expectedResponse.size());
    dstResponse.send(msg, expectedResponse.size());

    // Wait for sender thread
    if (senderThread.joinable()) {
        senderThread.join();
    }
}

TEST_CASE_METHOD(MessageContextFixture,
                 "Test send/recv many messages",
                 "[transport]")
{
    int numMessages = 10000;
    std::string baseMsg = "Hello ";

    std::thread senderThread([numMessages, baseMsg] {
        // Open the source endpoint client, don't bind
        SendMessageEndpoint src(thisHost, testPort);
        for (int i = 0; i < numMessages; i++) {
            std::string expectedMsg = baseMsg + std::to_string(i);
            uint8_t msg[expectedMsg.size()];
            memcpy(msg, expectedMsg.c_str(), expectedMsg.size());
            src.send(msg, expectedMsg.size());
        }
    });

    // Receive messages
    RecvMessageEndpoint dst(testPort);
    for (int i = 0; i < numMessages; i++) {
        faabric::transport::Message recvMsg = dst.recv();
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
        senderThreads.emplace_back(std::thread([numMessages, expectedMsg] {
            // Open the source endpoint client, don't bind
            SendMessageEndpoint src(thisHost, testPort);
            for (int i = 0; i < numMessages; i++) {
                uint8_t msg[expectedMsg.size()];
                memcpy(msg, expectedMsg.c_str(), expectedMsg.size());
                src.send(msg, expectedMsg.size());
            }
        }));
    }

    // Receive messages
    RecvMessageEndpoint dst(testPort);
    for (int i = 0; i < numSenders * numMessages; i++) {
        faabric::transport::Message recvMsg = dst.recv();
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
}
}
