#include <catch2/catch.hpp>

#include "faabric_utils.h"
#include "fixtures.h"

#include <faabric/mpi/MpiWorld.h>
#include <faabric/mpi/mpi.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/bytes.h>
#include <faabric/util/macros.h>
#include <faabric/util/random.h>

#include <thread>

using namespace faabric::mpi;
using namespace faabric::scheduler;

namespace tests {
TEST_CASE_METHOD(MpiBaseTestFixture, "Test world creation", "[mpi]")
{
    // Create the world (call the request first to make sure the planner has
    // the message stored)
    MpiWorld world;
    plannerCli.callFunctions(req);
    world.create(msg, worldId, worldSize);
    msg.set_ismpi(true);
    msg.set_mpiworldid(worldId);
    msg.set_mpirank(0);

    REQUIRE(world.getSize() == worldSize);
    REQUIRE(world.getId() == worldId);
    REQUIRE(world.getUser() == user);
    REQUIRE(world.getFunction() == func);

    // Wait to make sure all messages are scheduled and dispatched
    waitForMpiMessages(req, worldSize);

    // Check that chained function calls are made as expected
    std::vector<faabric::Message> actual = sch.getRecordedMessages();
    REQUIRE(actual.size() == worldSize);

    // Check all messages but the first one, which we only modify during
    // world creation (and not when we first call it)
    for (int i = 1; i < worldSize - 1; i++) {
        faabric::Message actualCall = actual.at(i + 1);
        REQUIRE(actualCall.user() == user);
        REQUIRE(actualCall.function() == func);
        REQUIRE(actualCall.ismpi());
        REQUIRE(actualCall.mpiworldid() == worldId);
        REQUIRE(actualCall.mpirank() == i + 1);
        REQUIRE(actualCall.mpiworldsize() == worldSize);
    }

    // Check that this host is registered as the main
    const std::string actualHost = world.getHostForRank(0);
    REQUIRE(actualHost == faabric::util::getSystemConfig().endpointHost);

    world.destroy();
}

TEST_CASE_METHOD(MpiBaseTestFixture, "Test creating world of size 1", "[mpi]")
{
    // Create a world of size 1
    MpiWorld world;
    int worldSize = 1;
    plannerCli.callFunctions(req);
    REQUIRE_NOTHROW(world.create(msg, worldId, worldSize));

    REQUIRE(world.getSize() == worldSize);
    REQUIRE(world.getId() == worldId);
    REQUIRE(world.getUser() == user);
    REQUIRE(world.getFunction() == func);

    // Check only one message is sent
    REQUIRE(sch.getRecordedMessages().size() == worldSize);

    world.destroy();
}

TEST_CASE_METHOD(MpiBaseTestFixture, "Test cartesian communicator", "[mpi]")
{
    MpiWorld world;
    int worldSize;
    int maxDims = 3;
    std::vector<int> dims(maxDims);
    std::vector<int> periods(maxDims, 1);
    std::vector<std::vector<int>> expectedShift;
    std::vector<std::vector<int>> expectedCoords;

    // Different grid sizes
    SECTION("5 x 1 grid")
    {
        // 5 processes create a 5x1 grid
        worldSize = 5;
        msg.set_mpiworldsize(worldSize);
        dims = { 5, 1, 1 };
        expectedCoords = {
            { 0, 0, 0 }, { 1, 0, 0 }, { 2, 0, 0 }, { 3, 0, 0 }, { 4, 0, 0 },
        };
        // We only test for the first three dimensions
        expectedShift = {
            { 4, 1, 0, 0, 0, 0 }, { 0, 2, 1, 1, 1, 1 }, { 1, 3, 2, 2, 2, 2 },
            { 2, 4, 3, 3, 3, 3 }, { 3, 0, 4, 4, 4, 4 },
        };
    }

    SECTION("2 x 2 grid")
    {
        // 4 processes create a 2x2 grid
        worldSize = 4;
        msg.set_mpiworldsize(worldSize);
        dims = { 2, 2, 1 };
        expectedCoords = {
            { 0, 0, 0 },
            { 0, 1, 0 },
            { 1, 0, 0 },
            { 1, 1, 0 },
        };
        // We only test for the first three dimensions
        expectedShift = {
            { 2, 2, 1, 1, 0, 0 },
            { 3, 3, 0, 0, 1, 1 },
            { 0, 0, 3, 3, 2, 2 },
            { 1, 1, 2, 2, 3, 3 },
        };
    }

    plannerCli.callFunctions(req);
    world.create(msg, worldId, worldSize);

    // Get coordinates from rank
    for (int i = 0; i < worldSize; i++) {
        std::vector<int> coords(3, -1);
        world.getCartesianRank(
          i, maxDims, dims.data(), periods.data(), coords.data());
        REQUIRE(expectedCoords[i] == coords);
    }

    // Get rank from coordinates
    for (int i = 0; i < dims[0]; i++) {
        for (int j = 0; j < dims[1]; j++) {
            int rank;
            std::vector<int> coords = { i, j, 0 };
            int expected =
              std::find(expectedCoords.begin(), expectedCoords.end(), coords) -
              expectedCoords.begin();
            world.getRankFromCoords(&rank, coords.data());
            REQUIRE(rank == expected);
        }
    }

    // Shift coordinates one unit along each axis
    for (int i = 0; i < dims[0]; i++) {
        for (int j = 0; j < dims[1]; j++) {
            std::vector<int> coords = { i, j, 0 };
            int rank;
            int src;
            int dst;
            world.getRankFromCoords(&rank, coords.data());
            // Test first dimension
            world.shiftCartesianCoords(rank, 0, 1, &src, &dst);
            REQUIRE(src == expectedShift[rank][0]);
            REQUIRE(dst == expectedShift[rank][1]);
            // Test second dimension
            world.shiftCartesianCoords(rank, 1, 1, &src, &dst);
            REQUIRE(src == expectedShift[rank][2]);
            REQUIRE(dst == expectedShift[rank][3]);
            // Test third dimension
            world.shiftCartesianCoords(rank, 2, 1, &src, &dst);
            REQUIRE(src == expectedShift[rank][4]);
            REQUIRE(dst == expectedShift[rank][5]);
        }
    }

    world.destroy();
}

TEST_CASE_METHOD(MpiBaseTestFixture, "Test local barrier", "[mpi]")
{
    // Create the world
    int worldSize = 2;
    MpiWorld world;
    plannerCli.callFunctions(req);
    world.create(msg, worldId, worldSize);

    int rankA1 = 0;
    int rankA2 = 1;
    std::vector<int> sendData = { 0, 1, 2 };
    std::vector<int> recvData = { -1, -1, -1 };

    std::jthread senderThread([&world, rankA1, rankA2, &sendData, &recvData] {
        world.send(
          rankA1, rankA2, BYTES(sendData.data()), MPI_INT, sendData.size());

        world.barrier(rankA1);
        assert(sendData == recvData);
    });

    world.recv(rankA1,
               rankA2,
               BYTES(recvData.data()),
               MPI_INT,
               recvData.size(),
               MPI_STATUS_IGNORE);

    REQUIRE(recvData == sendData);

    world.barrier(rankA2);

    senderThread.join();
    world.destroy();
}

void checkMessage(MPIMessage& actualMessage,
                  int worldId,
                  int senderRank,
                  int destRank,
                  const std::vector<int>& data)
{
    // Check the message contents
    REQUIRE(actualMessage.worldid() == worldId);
    REQUIRE(actualMessage.count() == data.size());
    REQUIRE(actualMessage.destination() == destRank);
    REQUIRE(actualMessage.sender() == senderRank);
    REQUIRE(actualMessage.type() == FAABRIC_INT);

    // Check data
    const auto* rawInts =
      reinterpret_cast<const int*>(actualMessage.buffer().c_str());
    size_t nInts = actualMessage.buffer().size() / sizeof(int);
    std::vector<int> actualData(rawInts, rawInts + nInts);
    REQUIRE(actualData == data);
}

TEST_CASE_METHOD(MpiTestFixture, "Test send and recv on same host", "[mpi]")
{
    // Send a message between colocated ranks
    int rankA1 = 0;
    int rankA2 = 1;
    std::vector<int> messageData = { 0, 1, 2 };
    world.send(
      rankA1, rankA2, BYTES(messageData.data()), MPI_INT, messageData.size());

    SECTION("Test queueing")
    {
        // Check the message itself is on the right queue
        REQUIRE(world.getLocalQueueSize(rankA1, rankA2) == 1);
        REQUIRE(world.getLocalQueueSize(rankA2, rankA1) == 0);
        REQUIRE(world.getLocalQueueSize(rankA1, 0) == 0);
        REQUIRE(world.getLocalQueueSize(rankA2, 0) == 0);

        // Check message content
        const std::shared_ptr<InMemoryMpiQueue>& queueA2 =
          world.getLocalQueue(rankA1, rankA2);
        MPIMessage actualMessage = *(queueA2->dequeue());
        checkMessage(actualMessage, worldId, rankA1, rankA2, messageData);
    }

    SECTION("Test recv")
    {
        // Receive the message
        MPI_Status status{};
        auto bufferAllocation = std::make_unique<int[]>(messageData.size());
        auto* buffer = bufferAllocation.get();
        world.recv(
          rankA1, rankA2, BYTES(buffer), MPI_INT, messageData.size(), &status);

        std::vector<int> actual(buffer, buffer + messageData.size());
        REQUIRE(actual == messageData);

        REQUIRE(status.MPI_ERROR == MPI_SUCCESS);
        REQUIRE(status.MPI_SOURCE == rankA1);
        REQUIRE(status.bytesSize == messageData.size() * sizeof(int));
    }
}

TEST_CASE_METHOD(MpiTestFixture, "Test sendrecv", "[mpi]")
{
    // Prepare data
    int rankA = 1;
    int rankB = 2;
    MPI_Status status{};
    std::vector<int> messageDataAB = { 0, 1, 2 };
    std::vector<int> messageDataBA = { 3, 2, 1, 0 };

    // Results
    std::vector<int> recvBufferA(messageDataBA.size(), 0);
    std::vector<int> recvBufferB(messageDataAB.size(), 0);

    // sendRecv is blocking, so we run two threads.
    // Run sendrecv from A
    std::vector<std::jthread> threads;
    threads.emplace_back([&] {
        world.sendRecv(BYTES(messageDataAB.data()),
                       messageDataAB.size(),
                       MPI_INT,
                       rankB,
                       BYTES(recvBufferA.data()),
                       messageDataBA.size(),
                       MPI_INT,
                       rankB,
                       rankA,
                       &status);
    });
    // Run sendrecv from B
    threads.emplace_back([&] {
        world.sendRecv(BYTES(messageDataBA.data()),
                       messageDataBA.size(),
                       MPI_INT,
                       rankA,
                       BYTES(recvBufferB.data()),
                       messageDataAB.size(),
                       MPI_INT,
                       rankA,
                       rankB,
                       &status);
    });

    // Wait for both to finish
    for (auto& t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }

    // Test integrity of results
    REQUIRE(recvBufferA == messageDataBA);
    REQUIRE(recvBufferB == messageDataAB);
}

TEST_CASE_METHOD(MpiTestFixture, "Test ring sendrecv", "[mpi]")
{
    // Use five processes
    std::vector<int> ranks = { 0, 1, 2, 3, 4 };

    // Prepare data
    MPI_Status status{};

    // Run shift operator. In a ring, send to right receive from left.
    std::vector<std::jthread> threads;
    for (int i = 0; i < ranks.size(); i++) {
        int rank = ranks[i];
        int left = rank > 0 ? rank - 1 : ranks.size() - 1;
        int right = (rank + 1) % ranks.size();
        threads.emplace_back([&, left, right, i] {
            int recvData = -1;
            int rank = ranks[i];
            world.sendRecv(BYTES(&rank),
                           1,
                           MPI_INT,
                           right,
                           BYTES(&recvData),
                           1,
                           MPI_INT,
                           left,
                           ranks[i],
                           &status);
            // Test integrity of results
            // TODO - no REQUIRE in the test case now
            assert(recvData == left);
        });
    }
    // Wait for all threads to finish
    for (auto& t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }
}

TEST_CASE_METHOD(MpiTestFixture, "Test async send and recv", "[mpi]")
{
    // Send a couple of async messages (from both to each other)
    int rankA = 1;
    int rankB = 2;
    std::vector<int> messageDataA = { 0, 1, 2 };
    std::vector<int> messageDataB = { 3, 4, 5, 6 };
    int sendIdA = world.isend(
      rankA, rankB, BYTES(messageDataA.data()), MPI_INT, messageDataA.size());
    int sendIdB = world.isend(
      rankB, rankA, BYTES(messageDataB.data()), MPI_INT, messageDataB.size());

    // Asynchronously do the receives
    std::vector<int> actualA(messageDataA.size(), 0);
    std::vector<int> actualB(messageDataB.size(), 0);
    int recvIdA =
      world.irecv(rankA, rankB, BYTES(actualA.data()), MPI_INT, actualA.size());
    int recvIdB =
      world.irecv(rankB, rankA, BYTES(actualB.data()), MPI_INT, actualB.size());

    // Await the results out of order (they should all complete)
    world.awaitAsyncRequest(recvIdB);
    world.awaitAsyncRequest(sendIdA);
    world.awaitAsyncRequest(recvIdA);
    world.awaitAsyncRequest(sendIdB);

    REQUIRE(actualA == messageDataA);
    REQUIRE(actualB == messageDataB);
}

TEST_CASE_METHOD(MpiTestFixture, "Test send/recv message with no data", "[mpi]")
{
    int rankA1 = 1;
    int rankA2 = 2;

    // Send a message between colocated ranks
    std::vector<int> messageData = { 0 };
    world.send(rankA1, rankA2, BYTES(messageData.data()), MPI_INT, 0);

    SECTION("Check on queue")
    {
        // Check message content
        MPIMessage actualMessage =
          *(world.getLocalQueue(rankA1, rankA2)->dequeue());
        REQUIRE(actualMessage.count() == 0);
        REQUIRE(actualMessage.type() == FAABRIC_INT);
    }

    SECTION("Check receiving with null ptr")
    {
        // Receiving with a null pointer shouldn't break
        MPI_Status status{};
        world.recv(rankA1, rankA2, nullptr, MPI_INT, 0, &status);

        REQUIRE(status.MPI_SOURCE == rankA1);
        REQUIRE(status.MPI_ERROR == MPI_SUCCESS);
        REQUIRE(status.bytesSize == 0);
    }
}

TEST_CASE_METHOD(MpiTestFixture, "Test recv with partial data", "[mpi]")
{
    // Send a message with size less than the recipient is expecting
    std::vector<int> messageData = { 0, 1, 2, 3 };
    unsigned long actualSize = messageData.size();
    world.send(1, 2, BYTES(messageData.data()), MPI_INT, actualSize);

    // Request to receive more values than were sent
    MPI_Status status{};
    unsigned long requestedSize = actualSize + 5;
    auto bufferAllocation = std::make_unique<int[]>(requestedSize);
    auto* buffer = bufferAllocation.get();
    world.recv(1, 2, BYTES(buffer), MPI_INT, requestedSize, &status);

    // Check status reports only the values that were sent
    REQUIRE(status.MPI_SOURCE == 1);
    REQUIRE(status.MPI_ERROR == MPI_SUCCESS);
    REQUIRE(status.bytesSize == actualSize * sizeof(int));
}

/*
 * 30/12/21 - MPI_Probe is broken after the switch to single-producer, single-
 * consumer fixed capacity queues.
TEST_CASE_METHOD(MpiTestFixture, "Test probe", "[.]")
{
    // Send two messages of different sizes
    std::vector<int> messageData = { 0, 1, 2, 3, 4, 5, 6 };
    unsigned long sizeA = 2;
    unsigned long sizeB = messageData.size();
    world.send(1, 2, BYTES(messageData.data()), MPI_INT, sizeA);
    world.send(1, 2, BYTES(messageData.data()), MPI_INT, sizeB);

    // Probe twice on the same message
    MPI_Status statusA1{};
    MPI_Status statusA2{};
    MPI_Status statusB{};
    world.probe(1, 2, &statusA1);
    world.probe(1, 2, &statusA2);

    // Check status reports only the values that were sent
    REQUIRE(statusA1.MPI_SOURCE == 1);
    REQUIRE(statusA1.MPI_ERROR == MPI_SUCCESS);
    REQUIRE(statusA1.bytesSize == sizeA * sizeof(int));

    REQUIRE(statusA2.MPI_SOURCE == 1);
    REQUIRE(statusA2.MPI_ERROR == MPI_SUCCESS);
    REQUIRE(statusA2.bytesSize == sizeA * sizeof(int));

    // Receive the message
    auto bufferAAllocation = std::make_unique<int[]>(sizeA);
    auto* bufferA = bufferAAllocation.get();
    world.recv(1, 2, BYTES(bufferA), MPI_INT, sizeA * sizeof(int), nullptr);

    // Probe the next message
    world.probe(1, 2, &statusB);
    REQUIRE(statusB.MPI_SOURCE == 1);
    REQUIRE(statusB.MPI_ERROR == MPI_SUCCESS);
    REQUIRE(statusB.bytesSize == sizeB * sizeof(int));

    // Receive the next message
    auto bufferBAllocation = std::make_unique<int[]>(sizeB);
    auto* bufferB = bufferBAllocation.get();
    world.recv(1, 2, BYTES(bufferB), MPI_INT, sizeB * sizeof(int), nullptr);
}
*/

TEST_CASE_METHOD(MpiTestFixture, "Check sending to invalid rank", "[mpi]")
{
    std::vector<int> input = { 0, 1, 2, 3 };
    int invalidRank = worldSize + 2;
    REQUIRE_THROWS(world.send(0, invalidRank, BYTES(input.data()), MPI_INT, 4));
}

TEST_CASE_METHOD(MpiTestFixture, "Test collective messaging locally", "[mpi]")
{
    int root = 3;

    SECTION("Broadcast")
    {
        // Broadcast a message from the root
        std::vector<int> messageData = { 0, 1, 2 };

        // First call from the root, which just sends
        world.broadcast(root,
                        root,
                        BYTES(messageData.data()),
                        MPI_INT,
                        messageData.size(),
                        MPIMessage::BROADCAST);

        // Recv on all non-root ranks
        for (int rank = 0; rank < worldSize; rank++) {
            if (rank == root) {
                continue;
            }
            std::vector<int> actual(3, -1);
            world.broadcast(root,
                            rank,
                            BYTES(actual.data()),
                            MPI_INT,
                            3,
                            MPIMessage::BROADCAST);
            REQUIRE(actual == messageData);
        }
    }

    // TODO - this is not scatter's behaviour, FIX
    SECTION("Scatter")
    {
        // Build the data
        int nPerRank = 4;
        std::vector<int> actual(nPerRank, -1);
        int dataSize = nPerRank * worldSize;
        std::vector<int> messageData(dataSize, 0);
        for (int i = 0; i < dataSize; i++) {
            messageData[i] = i;
        }

        // Do the root first
        world.scatter(root,
                      root,
                      BYTES(messageData.data()),
                      MPI_INT,
                      nPerRank,
                      BYTES(actual.data()),
                      MPI_INT,
                      nPerRank);

        for (int rank = 0; rank < worldSize; rank++) {
            // Do the scatter
            if (rank == root) {
                continue;
            }
            world.scatter(root,
                          rank,
                          BYTES(messageData.data()),
                          MPI_INT,
                          nPerRank,
                          BYTES(actual.data()),
                          MPI_INT,
                          nPerRank);

            // Check the results
            REQUIRE(actual == std::vector<int>(
                                messageData.begin() + rank * nPerRank,
                                messageData.begin() + (rank + 1) * nPerRank));
        }
    }

    SECTION("Gather and allgather")
    {
        // Build the data for each rank
        int nPerRank = 4;
        std::vector<std::vector<int>> rankData;
        for (int i = 0; i < worldSize; i++) {
            std::vector<int> thisRankData;
            for (int j = 0; j < nPerRank; j++) {
                thisRankData.push_back((i * nPerRank) + j);
            }

            rankData.push_back(thisRankData);
        }

        // Build the expectation
        std::vector<int> expected;
        for (int i = 0; i < worldSize * nPerRank; i++) {
            expected.push_back(i);
        }

        SECTION("Gather")
        {
            std::vector<int> actual(worldSize * nPerRank, -1);

            // Call gather for each rank other than the root (out of order)
            for (int rank = 0; rank < worldSize; rank++) {
                if (rank == root) {
                    continue;
                }
                world.gather(rank,
                             root,
                             BYTES(rankData[rank].data()),
                             MPI_INT,
                             nPerRank,
                             nullptr,
                             MPI_INT,
                             nPerRank);
            }

            // Call gather for root
            world.gather(root,
                         root,
                         BYTES(rankData[root].data()),
                         MPI_INT,
                         nPerRank,
                         BYTES(actual.data()),
                         MPI_INT,
                         nPerRank);

            // Check data
            REQUIRE(actual == expected);
        }
    }
}

template<typename T>
void doReduceTest(MpiWorld& world,
                  int root,
                  MPI_Op op,
                  MPI_Datatype datatype,
                  std::vector<std::vector<T>> rankData,
                  std::vector<T>& expected)
{
    int thisWorldSize = world.getSize();

    bool inPlace;
    SECTION("In place") { inPlace = true; }

    SECTION("Not in place") { inPlace = false; }

    // ---- Reduce ----
    // Call on all but the root first
    for (int r = 0; r < thisWorldSize; r++) {
        if (r == root) {
            continue;
        }
        world.reduce(
          r, root, BYTES(rankData[r].data()), nullptr, datatype, 3, op);
    }

    // Call on root to finish off and check
    std::vector<T> rootRankData = rankData[root];
    if (inPlace) {
        // In-place uses the same buffer for send and receive
        world.reduce(root,
                     root,
                     BYTES(rootRankData.data()),
                     BYTES(rootRankData.data()),
                     datatype,
                     3,
                     op);
        REQUIRE(rootRankData == expected);
    } else {
        // Not in-place uses a separate buffer for send and receive
        std::vector<T> actual(3, 0);
        world.reduce(root,
                     root,
                     BYTES(rootRankData.data()),
                     BYTES(actual.data()),
                     datatype,
                     3,
                     op);
        REQUIRE(actual == expected);
    }

    // ---- Allreduce ----
    // Run all as threads
    std::vector<std::jthread> threads;
    for (int r = 0; r < thisWorldSize; r++) {
        threads.emplace_back([&, r, inPlace] {
            std::vector<T> thisRankData = rankData[r];
            if (inPlace) {
                // In-place uses the same buffer for send and receive on _all_
                // hosts
                world.allReduce(r,
                                BYTES(thisRankData.data()),
                                BYTES(thisRankData.data()),
                                datatype,
                                3,
                                op);
                assert(thisRankData == expected);
            } else {
                std::vector<T> actual(3, 0);
                world.allReduce(r,
                                BYTES(thisRankData.data()),
                                BYTES(actual.data()),
                                datatype,
                                3,
                                op);
                assert(actual == expected);
            }
        });
    }

    for (auto& t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }
}

template void doReduceTest<int>(MpiWorld& world,
                                int root,
                                MPI_Op op,
                                MPI_Datatype datatype,
                                std::vector<std::vector<int>> rankData,
                                std::vector<int>& expected);

template void doReduceTest<double>(MpiWorld& world,
                                   int root,
                                   MPI_Op op,
                                   MPI_Datatype datatype,
                                   std::vector<std::vector<double>> rankData,
                                   std::vector<double>& expected);

TEST_CASE_METHOD(MpiTestFixture, "Test reduce", "[mpi]")
{
    // Prepare inputs
    int root = 3;

    SECTION("Integers")
    {
        std::vector<std::vector<int>> rankData(worldSize, std::vector<int>(3));
        std::vector<int> expected(3, 0);

        // Prepare rank data
        for (int r = 0; r < worldSize; r++) {
            rankData[r][0] = r;
            rankData[r][1] = r * 10;
            rankData[r][2] = r * 100;
        }

        SECTION("Sum operator")
        {
            for (int r = 0; r < worldSize; r++) {
                expected[0] += rankData[r][0];
                expected[1] += rankData[r][1];
                expected[2] += rankData[r][2];
            }

            doReduceTest<int>(
              world, root, MPI_SUM, MPI_INT, rankData, expected);
        }

        SECTION("Max operator")
        {
            expected[0] = (worldSize - 1);
            expected[1] = (worldSize - 1) * 10;
            expected[2] = (worldSize - 1) * 100;

            doReduceTest<int>(
              world, root, MPI_MAX, MPI_INT, rankData, expected);
        }

        SECTION("Min operator")
        {
            // Initialize rankData to non-zero values. This catches faulty
            // reduce implementations that always return zero
            for (int r = 0; r < worldSize; r++) {
                rankData[r][0] = (r + 1);
                rankData[r][1] = (r + 1) * 10;
                rankData[r][2] = (r + 1) * 100;
            }

            expected[0] = 1;
            expected[1] = 10;
            expected[2] = 100;

            doReduceTest<int>(
              world, root, MPI_MIN, MPI_INT, rankData, expected);
        }
    }

    SECTION("Doubles")
    {
        std::vector<std::vector<double>> rankData(worldSize,
                                                  std::vector<double>(3));
        std::vector<double> expected(3, 0);

        // Prepare rank data
        for (int r = 0; r < worldSize; r++) {
            rankData[r][0] = 2.5 + r;
            rankData[r][1] = (2.5 + r) * 10;
            rankData[r][2] = (2.5 + r) * 100;
        }

        SECTION("Sum operator")
        {
            for (int r = 0; r < worldSize; r++) {
                expected[0] += rankData[r][0];
                expected[1] += rankData[r][1];
                expected[2] += rankData[r][2];
            }

            doReduceTest<double>(
              world, root, MPI_SUM, MPI_DOUBLE, rankData, expected);
        }

        SECTION("Max operator")
        {
            expected[0] = (2.5 + worldSize - 1);
            expected[1] = (2.5 + worldSize - 1) * 10;
            expected[2] = (2.5 + worldSize - 1) * 100;

            doReduceTest<double>(
              world, root, MPI_MAX, MPI_DOUBLE, rankData, expected);
        }

        SECTION("Min operator")
        {
            expected[0] = 2.5;
            expected[1] = 25.0;
            expected[2] = 250.0;

            doReduceTest<double>(
              world, root, MPI_MIN, MPI_DOUBLE, rankData, expected);
        }
    }

    SECTION("Long long")
    {
        std::vector<std::vector<long long>> rankData(worldSize,
                                                     std::vector<long long>(3));
        std::vector<long long> expected(3, 0);

        // Prepare rank data
        for (int r = 0; r < worldSize; r++) {
            rankData[r][0] = (r + 1);
            rankData[r][1] = (r + 1) * 10;
            rankData[r][2] = (r + 1) * 100;
        }

        SECTION("Sum operator")
        {
            for (int r = 0; r < worldSize; r++) {
                expected[0] += rankData[r][0];
                expected[1] += rankData[r][1];
                expected[2] += rankData[r][2];
            }

            doReduceTest<long long>(
              world, root, MPI_SUM, MPI_DOUBLE, rankData, expected);
        }

        SECTION("Max operator")
        {
            expected[0] = worldSize;
            expected[1] = worldSize * 10;
            expected[2] = worldSize * 100;

            doReduceTest<long long>(
              world, root, MPI_MAX, MPI_DOUBLE, rankData, expected);
        }

        SECTION("Min operator")
        {
            expected[0] = 1;
            expected[1] = 10;
            expected[2] = 100;

            doReduceTest<long long>(
              world, root, MPI_MIN, MPI_DOUBLE, rankData, expected);
        }
    }
}

TEST_CASE_METHOD(MpiTestFixture, "Test operator reduce", "[mpi]")
{
    SECTION("Max")
    {
        SECTION("Integers")
        {
            std::vector<int> input = { 1, 1, 1 };
            std::vector<int> output = { 2, 2, 2 };
            std::vector<int> expected = { 2, 2, 2 };

            world.op_reduce(MPI_MAX,
                            MPI_INT,
                            3,
                            (uint8_t*)input.data(),
                            (uint8_t*)output.data());
            REQUIRE(output == expected);
        }

        SECTION("Doubles")
        {
            std::vector<double> input = { 2, 2, 2 };
            std::vector<double> output = { 1, 1, 1 };
            std::vector<double> expected = { 2, 2, 2 };

            world.op_reduce(MPI_MAX,
                            MPI_DOUBLE,
                            3,
                            (uint8_t*)input.data(),
                            (uint8_t*)output.data());
            REQUIRE(output == expected);
        }

        SECTION("Long long")
        {
            std::vector<long long> input = { 2, 2, 2 };
            std::vector<long long> output = { 1, 1, 1 };
            std::vector<long long> expected = { 2, 2, 2 };

            world.op_reduce(MPI_MAX,
                            MPI_LONG_LONG,
                            3,
                            (uint8_t*)input.data(),
                            (uint8_t*)output.data());
            REQUIRE(output == expected);
        }

        SECTION("Unsupported type")
        {
            std::vector<int> input = { 1, 1, 1 };
            std::vector<int> output = { 1, 1, 1 };

            REQUIRE_THROWS(world.op_reduce(MPI_MAX,
                                           MPI_DATATYPE_NULL,
                                           3,
                                           (uint8_t*)input.data(),
                                           (uint8_t*)output.data()));
        }
    }

    SECTION("Min")
    {
        SECTION("Integers")
        {
            std::vector<int> input = { 1, 1, 1 };
            std::vector<int> output = { 2, 2, 2 };
            std::vector<int> expected = { 1, 1, 1 };

            world.op_reduce(MPI_MIN,
                            MPI_INT,
                            3,
                            (uint8_t*)input.data(),
                            (uint8_t*)output.data());
            REQUIRE(output == expected);
        }

        SECTION("Doubles")
        {
            std::vector<double> input = { 2, 2, 2 };
            std::vector<double> output = { 1, 1, 1 };
            std::vector<double> expected = { 1, 1, 1 };

            world.op_reduce(MPI_MIN,
                            MPI_DOUBLE,
                            3,
                            (uint8_t*)input.data(),
                            (uint8_t*)output.data());
            REQUIRE(output == expected);
        }

        SECTION("Long long")
        {
            std::vector<long long> input = { 2, 2, 2 };
            std::vector<long long> output = { 1, 1, 1 };
            std::vector<long long> expected = { 1, 1, 1 };

            world.op_reduce(MPI_MIN,
                            MPI_LONG_LONG,
                            3,
                            (uint8_t*)input.data(),
                            (uint8_t*)output.data());
            REQUIRE(output == expected);
        }

        SECTION("Unsupported type")
        {
            std::vector<int> input = { 1, 1, 1 };
            std::vector<int> output = { 1, 1, 1 };

            REQUIRE_THROWS(world.op_reduce(MPI_MIN,
                                           MPI_DATATYPE_NULL,
                                           3,
                                           (uint8_t*)input.data(),
                                           (uint8_t*)output.data()));
        }
    }

    SECTION("Sum")
    {
        SECTION("Integers")
        {
            std::vector<int> input = { 1, 1, 1 };
            std::vector<int> output = { 1, 1, 1 };
            std::vector<int> expected = { 2, 2, 2 };

            world.op_reduce(MPI_SUM,
                            MPI_INT,
                            3,
                            (uint8_t*)input.data(),
                            (uint8_t*)output.data());
            REQUIRE(output == expected);
        }

        SECTION("Doubles")
        {
            std::vector<double> input = { 1, 1, 1 };
            std::vector<double> output = { 1, 1, 1 };
            std::vector<double> expected = { 2, 2, 2 };

            world.op_reduce(MPI_SUM,
                            MPI_DOUBLE,
                            3,
                            (uint8_t*)input.data(),
                            (uint8_t*)output.data());
            REQUIRE(output == expected);
        }

        SECTION("Long long")
        {
            std::vector<long long> input = { 1, 1, 1 };
            std::vector<long long> output = { 1, 1, 1 };
            std::vector<long long> expected = { 2, 2, 2 };

            world.op_reduce(MPI_SUM,
                            MPI_LONG_LONG,
                            3,
                            (uint8_t*)input.data(),
                            (uint8_t*)output.data());
            REQUIRE(output == expected);
        }

        SECTION("Unsupported type")
        {
            std::vector<int> input = { 1, 1, 1 };
            std::vector<int> output = { 1, 1, 1 };

            REQUIRE_THROWS(world.op_reduce(MPI_SUM,
                                           MPI_DATATYPE_NULL,
                                           3,
                                           (uint8_t*)input.data(),
                                           (uint8_t*)output.data()));
        }
    }
}

TEST_CASE_METHOD(MpiTestFixture, "Test gather and allgather", "[mpi]")
{
    int root = 3;

    // Build up per-rank data and expectation
    int nPerRank = 3;
    int gatheredSize = nPerRank * worldSize;
    std::vector<std::vector<int>> rankData(worldSize,
                                           std::vector<int>(nPerRank));
    std::vector<int> expected(gatheredSize, 0);
    for (int i = 0; i < gatheredSize; i++) {
        int thisRank = i / nPerRank;
        expected[i] = i;
        rankData[thisRank][i % nPerRank] = i;
    }

    // Prepare result buffer
    std::vector<int> actual(gatheredSize, 0);

    SECTION("Gather")
    {
        // Run gather on all non-root ranks
        for (int r = 0; r < worldSize; r++) {
            if (r == root) {
                continue;
            }
            world.gather(r,
                         root,
                         BYTES(rankData[r].data()),
                         MPI_INT,
                         nPerRank,
                         nullptr,
                         MPI_INT,
                         nPerRank);
        }

        SECTION("In place")
        {
            // With in-place gather we assume that the root's data is in the
            // correct place in the recv buffer already.
            std::copy(rankData[root].begin(),
                      rankData[root].end(),
                      actual.data() + (root * nPerRank));

            world.gather(root,
                         root,
                         BYTES(actual.data()),
                         MPI_INT,
                         nPerRank,
                         BYTES(actual.data()),
                         MPI_INT,
                         nPerRank);

            REQUIRE(actual == expected);
        }

        SECTION("Not in place")
        {
            world.gather(root,
                         root,
                         BYTES(rankData[root].data()),
                         MPI_INT,
                         nPerRank,
                         BYTES(actual.data()),
                         MPI_INT,
                         nPerRank);

            REQUIRE(actual == expected);
        }
    }

    SECTION("Allgather")
    {
        bool isInPlace;

        SECTION("In place") { isInPlace = true; }

        SECTION("Not in place") { isInPlace = false; }

        // Run allgather in threads
        std::vector<std::jthread> threads;
        for (int r = 0; r < worldSize; r++) {
            threads.emplace_back([&, r, isInPlace] {
                // Re-define the data vector so that each thread has its own
                // copy, avoiding data races
                std::vector<int> actual(gatheredSize, 0);
                if (isInPlace) {
                    // Put this rank's data in place in the recv buffer as
                    // expected
                    std::copy(rankData[r].begin(),
                              rankData[r].end(),
                              actual.data() + (r * nPerRank));

                    world.allGather(r,
                                    BYTES_CONST(actual.data()),
                                    MPI_INT,
                                    nPerRank,
                                    BYTES(actual.data()),
                                    MPI_INT,
                                    nPerRank);
                } else {
                    world.allGather(r,
                                    BYTES(rankData[r].data()),
                                    MPI_INT,
                                    nPerRank,
                                    BYTES(actual.data()),
                                    MPI_INT,
                                    nPerRank);
                }

                assert(actual == expected);
            });
        }

        for (auto& t : threads) {
            if (t.joinable()) {
                t.join();
            }
        }
    }
}

TEST_CASE_METHOD(MpiTestFixture, "Test scan", "[mpi]")
{
    int count = 3;

    // Prepare input data
    std::vector<std::vector<int>> rankData(worldSize, std::vector<int>(count));
    for (int r = 0; r < worldSize; r++) {
        for (int i = 0; i < count; i++) {
            rankData[r][i] = r * 10 + i;
        }
    }

    // Prepare expected values
    std::vector<std::vector<int>> expected(worldSize, std::vector<int>(count));
    for (int r = 0; r < worldSize; r++) {
        for (int i = 0; i < count; i++) {
            if (r == 0) {
                expected[r][i] = rankData[r][i];
            } else {
                expected[r][i] = expected[r - 1][i] + rankData[r][i];
            }
        }
    }

    bool inPlace;
    SECTION("In place") { inPlace = true; }
    SECTION("Not in place") { inPlace = false; }

    // Run the scan operation
    std::vector<std::vector<int>> result(worldSize, std::vector<int>(count));
    for (int r = 0; r < worldSize; r++) {
        if (inPlace) {
            world.scan(r,
                       BYTES(rankData[r].data()),
                       BYTES(rankData[r].data()),
                       MPI_INT,
                       count,
                       MPI_SUM);
            REQUIRE(rankData[r] == expected[r]);
        } else {
            world.scan(r,
                       BYTES(rankData[r].data()),
                       BYTES(result[r].data()),
                       MPI_INT,
                       count,
                       MPI_SUM);
            REQUIRE(result[r] == expected[r]);
        }
    }
}

TEST_CASE_METHOD(MpiBaseTestFixture, "Test all-to-all", "[mpi]")
{
    // For this test we need a fixed world size of 4, otherwise the built
    // expectation won't match. Thus, we use the base test fixture
    int worldSize = 4;
    msg.set_mpiworldsize(worldSize);
    MpiWorld world;
    plannerCli.callFunctions(req);
    world.create(msg, worldId, worldSize);

    // Build inputs and expected
    int inputs[4][8] = {
        { 0, 1, 2, 3, 4, 5, 6, 7 },
        { 10, 11, 12, 13, 14, 15, 16, 17 },
        { 20, 21, 22, 23, 24, 25, 26, 27 },
        { 30, 31, 32, 33, 34, 35, 36, 37 },
    };

    int expected[4][8] = {
        { 0, 1, 10, 11, 20, 21, 30, 31 },
        { 2, 3, 12, 13, 22, 23, 32, 33 },
        { 4, 5, 14, 15, 24, 25, 34, 35 },
        { 6, 7, 16, 17, 26, 27, 36, 37 },
    };

    std::vector<std::jthread> threads;
    for (int r = 0; r < worldSize; r++) {
        threads.emplace_back([&, r] {
            std::vector<int> actual(8, 0);
            world.allToAll(r,
                           BYTES(inputs[r]),
                           MPI_INT,
                           2,
                           BYTES(actual.data()),
                           MPI_INT,
                           2);

            std::vector<int> thisExpected(expected[r], expected[r] + 8);
            assert(actual == thisExpected);
        });
    }

    for (auto& t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }

    world.destroy();
}

TEST_CASE_METHOD(MpiTestFixture,
                 "Test can't destroy world with outstanding requests",
                 "[mpi]")
{
    int rankA = 0;
    int rankB = 1;
    int data = 9;
    int actual = -1;

    SECTION("Outstanding irecv")
    {
        world.send(rankA, rankB, BYTES(&data), MPI_INT, 1);
        int recvId = world.irecv(rankA, rankB, BYTES(&actual), MPI_INT, 1);

        REQUIRE_THROWS(world.destroy());

        world.awaitAsyncRequest(recvId);
        REQUIRE(actual == data);
    }

    SECTION("Outstanding acknowledged irecv")
    {
        int data2 = 14;
        int actual2 = -1;

        world.send(rankA, rankB, BYTES(&data), MPI_INT, 1);
        world.send(rankA, rankB, BYTES(&data2), MPI_INT, 1);
        int recvId = world.irecv(rankA, rankB, BYTES(&actual), MPI_INT, 1);
        int recvId2 = world.irecv(rankA, rankB, BYTES(&actual2), MPI_INT, 1);

        REQUIRE_THROWS(world.destroy());

        // Await for the second request, which will acknowledge the first one
        // but not remove it from the pending message buffer
        world.awaitAsyncRequest(recvId2);

        REQUIRE_THROWS(world.destroy());

        // Await for the first one
        world.awaitAsyncRequest(recvId);

        REQUIRE(actual == data);
        REQUIRE(actual2 == data2);
    }

    SECTION("Outstanding isend")
    {
        int sendId = world.isend(rankA, rankB, BYTES(&data), MPI_INT, 1);
        world.recv(rankA, rankB, BYTES(&actual), MPI_INT, 1, MPI_STATUS_IGNORE);

        REQUIRE_THROWS(world.destroy());

        world.awaitAsyncRequest(sendId);
        REQUIRE(actual == data);
    }
}
}
