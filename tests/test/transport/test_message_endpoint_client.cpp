#include "faabric_utils.h"
#include <catch2/catch.hpp>

#include <atomic>
#include <thread>
#include <unistd.h>

#include <faabric/transport/MessageEndpoint.h>
#include <faabric/util/gids.h>
#include <faabric/util/latch.h>
#include <faabric/util/macros.h>

using namespace faabric::transport;

#define TEST_PORT 9800

namespace tests {

// These tests are unstable under ThreadSanitizer
#if !(defined(__has_feature) && __has_feature(thread_sanitizer))

TEST_CASE_METHOD(SchedulerTestFixture,
                 "Test send/recv one message",
                 "[transport]")
{
    AsyncSendMessageEndpoint src(LOCALHOST, TEST_PORT);
    AsyncRecvMessageEndpoint dst(TEST_PORT);

    // Send message
    std::string expectedMsg = "Hello world!";
    const uint8_t* msg = BYTES_CONST(expectedMsg.c_str());
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

    AsyncSendMessageEndpoint src(LOCALHOST, TEST_PORT);

    auto latch = faabric::util::Latch::create(2);

    std::thread recvThread([&latch, expectedMsg] {
        // Make sure this only runs once the send has been done
        latch->wait();

        // Receive message
        AsyncRecvMessageEndpoint dst(TEST_PORT);
        faabric::transport::Message recvMsg = dst.recv();

        assert(recvMsg.size() == expectedMsg.size());
        std::string actualMsg(recvMsg.data(), recvMsg.size());
        assert(actualMsg == expectedMsg);
    });

    const uint8_t* msg = BYTES_CONST(expectedMsg.c_str());
    src.send(msg, expectedMsg.size());
    latch->wait();

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
        SyncSendMessageEndpoint src(LOCALHOST, TEST_PORT);

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
    SyncRecvMessageEndpoint dst(TEST_PORT);
    faabric::transport::Message recvMsg = dst.recv();
    REQUIRE(recvMsg.size() == expectedMsg.size());
    std::string actualMsg(recvMsg.data(), recvMsg.size());
    REQUIRE(actualMsg == expectedMsg);

    // Send response
    const uint8_t* msg = BYTES_CONST(expectedResponse.c_str());
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
        AsyncSendMessageEndpoint src(LOCALHOST, TEST_PORT);
        for (int i = 0; i < numMessages; i++) {
            std::string msgData = baseMsg + std::to_string(i);
            const uint8_t* msg = BYTES_CONST(msgData.c_str());
            src.send(msg, msgData.size());
        }
    });

    // Receive messages
    AsyncRecvMessageEndpoint dst(TEST_PORT);
    for (int i = 0; i < numMessages; i++) {
        faabric::transport::Message recvMsg = dst.recv();
        // Check just a subset of the messages
        // This implicitly tests in-order message delivery
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
    const uint8_t* msg = BYTES_CONST(expectedMsg.c_str());

    for (int j = 0; j < numSenders; j++) {
        senderThreads.emplace_back(std::thread([msg, numMessages, expectedMsg] {
            // Open the source endpoint client
            AsyncSendMessageEndpoint src(LOCALHOST, TEST_PORT);
            for (int i = 0; i < numMessages; i++) {
                src.send(msg, expectedMsg.size());
            }
        }));
    }

    // Receive messages
    AsyncRecvMessageEndpoint dst(TEST_PORT);
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
        AsyncSendMessageEndpoint s(LOCALHOST, TEST_PORT, 100);
        AsyncRecvMessageEndpoint r(TEST_PORT, 100);

        SyncSendMessageEndpoint sB(LOCALHOST, TEST_PORT + 10, 100);
        SyncRecvMessageEndpoint rB(TEST_PORT + 10, 100);
    }

    SECTION("Recv zero timeout")
    {
        REQUIRE_THROWS(AsyncRecvMessageEndpoint(TEST_PORT, 0));
        REQUIRE_THROWS(SyncRecvMessageEndpoint(TEST_PORT + 10, 0));
    }

    SECTION("Send zero timeout")
    {
        REQUIRE_THROWS(AsyncSendMessageEndpoint(LOCALHOST, TEST_PORT, 0));
        REQUIRE_THROWS(SyncSendMessageEndpoint(LOCALHOST, TEST_PORT + 10, 0));
    }

    SECTION("Recv negative timeout")
    {
        REQUIRE_THROWS(AsyncRecvMessageEndpoint(TEST_PORT, -1));
        REQUIRE_THROWS(SyncRecvMessageEndpoint(TEST_PORT + 10, -1));
    }

    SECTION("Send negative timeout")
    {
        REQUIRE_THROWS(AsyncSendMessageEndpoint(LOCALHOST, TEST_PORT, -1));
        REQUIRE_THROWS(SyncSendMessageEndpoint(LOCALHOST, TEST_PORT + 10, -1));
    }
}

TEST_CASE_METHOD(SchedulerTestFixture, "Test direct messaging", "[transport]")
{
    std::string expected = "Direct hello";
    const uint8_t* msg = BYTES_CONST(expected.c_str());

    std::string inprocLabel =
      "direct-test-" + std::to_string(faabric::util::generateGid());

    // Send the message
    AsyncDirectSendEndpoint sender(inprocLabel);
    sender.send(msg, expected.size());

    AsyncDirectRecvEndpoint receiver(inprocLabel);

    std::string actual;
    SECTION("Recv with size")
    {
        faabric::transport::Message recvMsg = receiver.recv(expected.size());
        actual = std::string(recvMsg.data(), recvMsg.size());
    }

    SECTION("Recv no size")
    {
        faabric::transport::Message recvMsg = receiver.recv();
        actual = std::string(recvMsg.data(), recvMsg.size());
    }

    REQUIRE(actual == expected);
}

TEST_CASE_METHOD(SchedulerTestFixture,
                 "Stress test direct messaging",
                 "[transport]")
{
    int nMessages = 1000;
    int nPairs = 3;
    std::string inprocLabel = "direct-test-";

    std::shared_ptr<faabric::util::Latch> startLatch =
      faabric::util::Latch::create(nPairs + 1);

    std::vector<std::thread> senders;
    std::vector<std::thread> receivers;

    for (int i = 0; i < nPairs; i++) {
        senders.emplace_back([i, nMessages, inprocLabel, &startLatch] {
            std::string thisLabel = inprocLabel + std::to_string(i);
            AsyncDirectSendEndpoint sender(thisLabel);

            for (int m = 0; m < nMessages; m++) {
                std::string expected =
                  "Direct hello " + std::to_string(i) + "_" + std::to_string(m);
                const uint8_t* msg = BYTES_CONST(expected.c_str());
                sender.send(msg, expected.size());

                if (m % 100 == 0) {
                    SLEEP_MS(10);
                }

                // Make main thread wait until messages are queued (to check no
                // issue with connecting before binding)
                if (m == 10) {
                    startLatch->wait();
                }
            }
        });
    }

    // Wait for queued messages
    startLatch->wait();

    std::atomic<bool> success = true;
    for (int i = 0; i < nPairs; i++) {
        receivers.emplace_back([i, nMessages, inprocLabel, &success] {
            std::string thisLabel = inprocLabel + std::to_string(i);
            AsyncDirectRecvEndpoint receiver(thisLabel);

            // Receive messages
            for (int m = 0; m < nMessages; m++) {
                faabric::transport::Message recvMsg = receiver.recv();
                std::string actual(recvMsg.data(), recvMsg.size());

                std::string expected =
                  "Direct hello " + std::to_string(i) + "_" + std::to_string(m);

                if (actual != expected) {
                    success.store(false);
                }
            }
        });
    }

    REQUIRE(success.load(std::memory_order_acquire));

    for (auto& t : senders) {
        if (t.joinable()) {
            t.join();
        }
    }

    for (auto& t : receivers) {
        if (t.joinable()) {
            t.join();
        }
    }
}

#endif // End ThreadSanitizer exclusion

}
