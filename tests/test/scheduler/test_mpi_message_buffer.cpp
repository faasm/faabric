#include <catch.hpp>

#include <faabric/mpi/mpi.h>
#include <faabric/scheduler/MpiMessageBuffer.h>
#include <faabric/util/gids.h>

using namespace faabric::scheduler;

MpiMessageBuffer::Arguments genRandomArguments(bool nullMsg = true,
                                               int overrideRequestId = -1)
{
    int requestId;
    if (overrideRequestId != -1) {
        requestId = overrideRequestId;
    } else {
        requestId = static_cast<int>(faabric::util::generateGid());
    }

    MpiMessageBuffer::Arguments args = {
        requestId, nullptr, 0, 1,
        nullptr,   MPI_INT, 0, faabric::MPIMessage::NORMAL
    };

    if (!nullMsg) {
        args.msg = std::make_shared<faabric::MPIMessage>();
    }

    return args;
}

namespace tests {
TEST_CASE("Test adding message to message buffer", "[mpi]")
{
    MpiMessageBuffer mmb;
    REQUIRE(mmb.isEmpty());

    REQUIRE_NOTHROW(mmb.addMessage(genRandomArguments()));
    REQUIRE(mmb.size() == 1);
}

TEST_CASE("Test deleting message from message buffer", "[mpi]")
{
    MpiMessageBuffer mmb;
    REQUIRE(mmb.isEmpty());

    mmb.addMessage(genRandomArguments());
    REQUIRE(mmb.size() == 1);

    auto it = mmb.getFirstNullMsg();
    REQUIRE_NOTHROW(mmb.deleteMessage(it));

    REQUIRE(mmb.isEmpty());
}

TEST_CASE("Test getting an iterator from a request id", "[mpi]")
{
    MpiMessageBuffer mmb;

    int requestId = 1337;
    mmb.addMessage(genRandomArguments(true, requestId));

    auto it = mmb.getRequestArguments(requestId);
    REQUIRE(it->requestId == requestId);
}

TEST_CASE("Test getting first null message", "[mpi]")
{
    MpiMessageBuffer mmb;

    // Add first a non-null message
    int requestId1 = 1;
    mmb.addMessage(genRandomArguments(false, requestId1));

    // Then add a null message
    int requestId2 = 2;
    mmb.addMessage(genRandomArguments(true, requestId2));

    // Query for the first non-null message
    auto it = mmb.getFirstNullMsg();
    REQUIRE(it->requestId == requestId2);
}

TEST_CASE("Test getting total unacked messages in message buffer", "[mpi]")
{
    MpiMessageBuffer mmb;

    REQUIRE(mmb.getTotalUnackedMessages() == 0);

    // Add a non-null message
    mmb.addMessage(genRandomArguments(false));

    // Then a couple of null messages
    mmb.addMessage(genRandomArguments(true));
    mmb.addMessage(genRandomArguments(true));

    // Check that we have two unacked messages
    REQUIRE(mmb.getTotalUnackedMessages() == 2);
}

TEST_CASE("Test getting total unacked messages in message buffer range",
          "[mpi]")
{
    MpiMessageBuffer mmb;

    // Add a non-null message
    mmb.addMessage(genRandomArguments(false));

    // Then a couple of null messages
    int requestId = 1337;
    mmb.addMessage(genRandomArguments(true));
    mmb.addMessage(genRandomArguments(true, requestId));

    // Get an iterator to our second null message
    auto it = mmb.getRequestArguments(requestId);

    // Check that we have only one unacked message until the iterator
    REQUIRE(mmb.getTotalUnackedMessagesUntil(it) == 1);
}
}
