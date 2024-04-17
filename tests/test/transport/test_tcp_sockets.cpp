#include <catch2/catch.hpp>

#include <faabric/transport/tcp/RecvSocket.h>
#include <faabric/transport/tcp/SendSocket.h>
#include <faabric/transport/tcp/SocketOptions.h>
#include <faabric/util/macros.h>
#include <faabric/util/network.h>

#include <latch>
#include <thread>

using namespace faabric::transport::tcp;

#define TEST_PORT 9999

namespace tests {
TEST_CASE("Test connecting a send/recv socket pair", "[transport]")
{
    int conn;
    {
        RecvSocket dst(TEST_PORT);

        // Start sending socket thread
        std::jthread sendThread([&] {
            SendSocket src(LOCALHOST, TEST_PORT);
            // Connect to receiving socket
            src.dial();
        });

        // Connect to send socket and store the connection fd
        dst.listen();
        conn = dst.accept();
    }

    // Verify that the open connections are closed when the sockets go out
    // of scope
    ::close(conn);
    REQUIRE(errno == EBADF);
}

TEST_CASE("Test setting socket options", "[transport]")
{
    std::latch endLatch(2);

    int conn;
    {
        RecvSocket dst(TEST_PORT);

        // Start sending socket thread
        std::jthread sendThread([&] {
            SendSocket src(LOCALHOST, TEST_PORT);

            // Connect to receiving socket
            src.dial();

            endLatch.arrive_and_wait();
        });

        // Connect to send socket and store the connection fd
        dst.listen();
        conn = dst.accept();

        // Set options on the socket given by accept (i.e. the recv socket)
        setReuseAddr(conn);
        setNoDelay(conn);
        setQuickAck(conn);
        setQuickAck(conn);
        setBusyPolling(conn);
        setNonBlocking(conn);
        setBlocking(conn);
        setRecvTimeoutMs(conn, SocketTimeoutMs);
        setSendTimeoutMs(conn, SocketTimeoutMs);
        setRecvBufferSize(conn, SocketBufferSizeBytes);
        setSendBufferSize(conn, SocketBufferSizeBytes);

        REQUIRE(!isNonBlocking(conn));

        endLatch.arrive_and_wait();
    }

    // Verify that the open connections are closed when the sockets go out
    // of scope
    ::close(conn);
    REQUIRE(errno == EBADF);

    // Any operation on the closed socket will fail
    REQUIRE_THROWS(setReuseAddr(conn));
    REQUIRE_THROWS(setNoDelay(conn));
    REQUIRE_THROWS(setQuickAck(conn));
    REQUIRE_THROWS(setQuickAck(conn));
    REQUIRE_THROWS(setBusyPolling(conn));
    REQUIRE_THROWS(setNonBlocking(conn));
    REQUIRE_THROWS(setBlocking(conn));
    REQUIRE_THROWS(setRecvTimeoutMs(conn, SocketTimeoutMs));
    REQUIRE_THROWS(setSendTimeoutMs(conn, SocketTimeoutMs));
    REQUIRE_THROWS(setRecvBufferSize(conn, SocketBufferSizeBytes));
    REQUIRE_THROWS(setSendBufferSize(conn, SocketBufferSizeBytes));
}

TEST_CASE("Test send/recv one message using raw TCP sockets", "[transport]")
{
    RecvSocket dst(TEST_PORT);
    std::latch endLatch(2);

    std::vector<int> msg;
    bool timeout = false;

    SECTION("Small message")
    {
        msg = std::vector<int>(3, 2);
    }

    SECTION("Large message")
    {
        msg = std::vector<int>(300, 200);
    }

    SECTION("Do not send a message")
    {
        msg = std::vector<int>(3, 2);
        timeout = true;
    }

    // Start sending socket thread
    std::jthread sendThread([&] {
        SendSocket src(LOCALHOST, TEST_PORT);
        // Connect to receiving socket
        src.dial();

        if (!timeout) {
            src.sendOne(BYTES(msg.data()), sizeof(int) * msg.size());
        }

        endLatch.arrive_and_wait();
    });

    // Prepare receiving socket
    dst.listen();
    int conn = dst.accept();

    std::vector<int> actual(msg.size());

    // To test the timeout we must set the socket as blocking
    if (timeout) {
        setRecvTimeoutMs(conn, 200);
        REQUIRE_THROWS(
          dst.recvOne(conn, BYTES(actual.data()), sizeof(int) * actual.size()));
    } else {
        setRecvBufferSize(conn, SocketBufferSizeBytes);

        dst.recvOne(conn, BYTES(actual.data()), sizeof(int) * actual.size());

        REQUIRE(actual == msg);
    }

    endLatch.arrive_and_wait();
}
}
