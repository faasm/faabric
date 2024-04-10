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

        // Set options on the socket given by accept (i.e. the send socket)
        setReuseAddr(conn);
        setNoDelay(conn);
        setQuickAck(conn);
        setQuickAck(conn);
        setBusyPolling(conn);
        setNonBlocking(conn);
        setBlocking(conn);

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
}

TEST_CASE("Test send/recv one message using raw TCP sockets", "[transport]")
{
    RecvSocket dst(TEST_PORT);

    std::vector<int> msg;

    SECTION("Small message")
    {
        msg = std::vector<int>(3, 2);
    }

    SECTION("Large message")
    {
        msg = std::vector<int>(300, 200);
    }

    // Start sending socket thread
    std::jthread sendThread([&] {
        SendSocket src(LOCALHOST, TEST_PORT);
        // Connect to receiving socket
        src.dial();

        src.sendOne(BYTES(msg.data()), sizeof(int) * msg.size());
    });

    // Prepare receiving socket
    dst.listen();
    int conn = dst.accept();

    std::vector<int> actual(msg.size());
    dst.recvOne(conn, BYTES(actual.data()), sizeof(int) * actual.size());

    REQUIRE(actual == msg);
}

TEST_CASE("Test using scatter send functionality (sendMany)", "[transport]")
{
    RecvSocket dst(TEST_PORT);

    std::vector<std::vector<int>> msgs;

    SECTION("One message")
    {
        msgs = { std::vector<int>(3, 2) };
    }

    SECTION("Two messages")
    {
        msgs = { std::vector<int>(3, 1), std::vector<int>(3, 2) };
    }

    // Pre-define all ararys for all sections to avoid VLA errors
    std::array<uint8_t*, 1> bufferArray1;
    std::array<size_t, 1> sizeArray1;
    std::array<uint8_t*, 2> bufferArray2;
    std::array<size_t, 2> sizeArray2;

    // Start sending socket thread
    std::jthread sendThread([&] {
        SendSocket src(LOCALHOST, TEST_PORT);
        // Connect to receiving socket
        src.dial();

        // Prepare the sending arrays
        if (msgs.size() == 1) {
            bufferArray1 = { BYTES(msgs.at(0).data()) };
            sizeArray1 = { sizeof(int) * msgs.at(0).size() };

            src.sendMany<1>(bufferArray1, sizeArray1);
        } else if (msgs.size() == 2) {
            bufferArray2 = { BYTES(msgs.at(0).data()),
                             BYTES(msgs.at(1).data()) };
            sizeArray2 = { sizeof(int) * msgs.at(0).size(),
                           sizeof(int) * msgs.at(1).size() };

            src.sendMany<2>(bufferArray2, sizeArray2);
        }
    });

    // Prepare receiving socket
    dst.listen();
    int conn = dst.accept();

    if (msgs.size() == 1) {
        std::vector<int> actual(msgs.at(0).size());
        dst.recvOne(conn, BYTES(actual.data()), sizeof(int) * actual.size());

        REQUIRE(actual == msgs.at(0));
    } else if (msgs.size() == 2) {
        std::vector<std::vector<int>> actual = {
            std::vector<int>(msgs.at(0).size()),
            std::vector<int>(msgs.at(1).size())
        };
        dst.recvOne(
          conn, BYTES(actual.at(0).data()), sizeof(int) * actual.at(0).size());
        dst.recvOne(
          conn, BYTES(actual.at(1).data()), sizeof(int) * actual.at(1).size());

        REQUIRE(actual == msgs);
    }
}
}
