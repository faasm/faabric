#include "faabric_utils.h"
#include <catch.hpp>

#include <thread>
#include <unistd.h>

#include <faabric/transport/MessageEndpoint.h>
#include <faabric/util/macros.h>

using namespace faabric::transport;
static const std::string thisHost = "127.0.0.1";
static const int testPort = 9800;

namespace tests {

TEST_CASE_METHOD(SchedulerTestFixture,
                 "Test send/recv one message",
                 "[transport]")
{
    AsyncSendMessageEndpoint src(thisHost, testPort);
    AsyncRecvMessageEndpoint dst(testPort);

    // Send message
    std::string expectedMsg = "Hello world!";
    uint8_t msg[expectedMsg.size()];
    memcpy(msg, expectedMsg.c_str(), expectedMsg.size());
    src.send(msg, expectedMsg.size());

    // Receive message
    faabric::transport::Message recvMsg = dst.recv();
    REQUIRE(recvMsg.size() == expectedMsg.size());
    std::string actualMsg(recvMsg.data(), recvMsg.size());
    REQUIRE(actualMsg == expectedMsg);
}

TEST_CASE_METHOD(SchedulerTestFixture,
                 "Test send before recv is ready",
                 "[transport]")
{
    std::string expectedMsg = "Hello world!";

    AsyncSendMessageEndpoint src(thisHost, testPort);

    faabric::util::Barrier barrier(2);

    std::thread recvThread([&barrier, expectedMsg] {
        // Make sure this only runs once the send has been done
        barrier.wait();

        // Receive message
        AsyncRecvMessageEndpoint dst(testPort);
        faabric::transport::Message recvMsg = dst.recv();

        assert(recvMsg.size() == expectedMsg.size());
        std::string actualMsg(recvMsg.data(), recvMsg.size());
        assert(actualMsg == expectedMsg);
    });

    uint8_t msg[expectedMsg.size()];
    memcpy(msg, expectedMsg.c_str(), expectedMsg.size());

    src.send(msg, expectedMsg.size());
    barrier.wait();

    if (recvThread.joinable()) {
        recvThread.join();
    }
}

TEST_CASE_METHOD(SchedulerTestFixture, "Test await response", "[transport]")
{
    // Prepare common message/response
    std::string expectedMsg = "Hello ";
    std::string expectedResponse = "world!";

    std::thread senderThread([expectedMsg, expectedResponse] {
        // Open the source endpoint client
        SyncSendMessageEndpoint src(thisHost, testPort);

        // Send message and wait for response
        std::vector<uint8_t> bytes(BYTES_CONST(expectedMsg.c_str()),
                                   BYTES_CONST(expectedMsg.c_str()) +
                                     expectedMsg.size());

        faabric::transport::Message recvMsg =
          src.sendAwaitResponse(bytes.data(), bytes.size());

        // Block waiting for a response
        assert(recvMsg.size() == expectedResponse.size());
        std::string actualResponse(recvMsg.data(), recvMsg.size());
        assert(actualResponse == expectedResponse);
    });

    // Receive message
    SyncRecvMessageEndpoint dst(testPort);
    faabric::transport::Message recvMsg = dst.recv();
    REQUIRE(recvMsg.size() == expectedMsg.size());
    std::string actualMsg(recvMsg.data(), recvMsg.size());
    REQUIRE(actualMsg == expectedMsg);

    // Send response
    uint8_t msg[expectedResponse.size()];
    memcpy(msg, expectedResponse.c_str(), expectedResponse.size());
    dst.sendResponse(msg, expectedResponse.size());

    // Wait for sender thread
    if (senderThread.joinable()) {
        senderThread.join();
    }
}

TEST_CASE_METHOD(SchedulerTestFixture,
                 "Test send/recv many messages",
                 "[transport]")
{
    int numMessages = 10000;
    std::string baseMsg = "Hello ";

    std::thread senderThread([numMessages, baseMsg] {
        // Open the source endpoint client
        AsyncSendMessageEndpoint src(thisHost, testPort);
        for (int i = 0; i < numMessages; i++) {
            std::string msgData = baseMsg + std::to_string(i);
            uint8_t msg[msgData.size()];
            memcpy(msg, msgData.c_str(), msgData.size());
            src.send(msg, msgData.size());
        }
    });

    // Receive messages
    AsyncRecvMessageEndpoint dst(testPort);
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

TEST_CASE_METHOD(SchedulerTestFixture,
                 "Test send/recv many messages from many clients",
                 "[transport]")
{
    int numMessages = 10000;
    int numSenders = 10;
    std::string expectedMsg = "Hello from client";
    std::vector<std::thread> senderThreads;

    for (int j = 0; j < numSenders; j++) {
        senderThreads.emplace_back(std::thread([numMessages, expectedMsg] {
            // Open the source endpoint client
            AsyncSendMessageEndpoint src(thisHost, testPort);
            for (int i = 0; i < numMessages; i++) {
                uint8_t msg[expectedMsg.size()];
                memcpy(msg, expectedMsg.c_str(), expectedMsg.size());
                src.send(msg, expectedMsg.size());
            }
        }));
    }

    // Receive messages
    AsyncRecvMessageEndpoint dst(testPort);
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

TEST_CASE_METHOD(SchedulerTestFixture,
                 "Test can't set invalid send/recv timeouts",
                 "[transport]")
{

    SECTION("Sanity check valid timeout")
    {
        AsyncSendMessageEndpoint s(thisHost, testPort, 100);
        AsyncRecvMessageEndpoint r(testPort, 100);

        SyncSendMessageEndpoint sB(thisHost, testPort + 10, 100);
        SyncRecvMessageEndpoint rB(testPort + 10, 100);
    }

    SECTION("Recv zero timeout")
    {
        REQUIRE_THROWS(AsyncRecvMessageEndpoint(testPort, 0));
        REQUIRE_THROWS(SyncRecvMessageEndpoint(testPort + 10, 0));
    }

    SECTION("Send zero timeout")
    {
        REQUIRE_THROWS(AsyncSendMessageEndpoint(thisHost, testPort, 0));
        REQUIRE_THROWS(SyncSendMessageEndpoint(thisHost, testPort + 10, 0));
    }

    SECTION("Recv negative timeout")
    {
        REQUIRE_THROWS(AsyncRecvMessageEndpoint(testPort, -1));
        REQUIRE_THROWS(SyncRecvMessageEndpoint(testPort + 10, -1));
    }

    SECTION("Send negative timeout")
    {
        REQUIRE_THROWS(AsyncSendMessageEndpoint(thisHost, testPort, -1));
        REQUIRE_THROWS(SyncSendMessageEndpoint(thisHost, testPort + 10, -1));
    }
}
}
