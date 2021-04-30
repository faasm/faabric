#include <catch.hpp>
#include <zmq.hpp>

#include <thread>

#include <faabric_utils.h>

#include <faabric/proto/mpiMessage.pb.h>
#include <faabric/rpc/macros.h>
#include <faabric/scheduler/MpiWorld.h>
#include <faabric/scheduler/MpiWorldRegistry.h>
#include <faabric/transport/MpiMessageEndpoint.h>

#include <faabric/util/logging.h>

using namespace faabric::transport;

namespace tests {
TEST_CASE("Test connect two sockets", "[transport]")
{
    // zmq::context_t context(1);
    MessageContext context;

    {
        MpiMessageEndpoint origin, destination;
        origin.start(context, SocketType::PUSH, true);
        REQUIRE_NOTHROW(destination.start(context, SocketType::PULL, false));
    }

    context.close();
}

TEST_CASE("Test send one MPI message", "[transport]")
{
    cleanFaabric();

    // Prepare MPI World
    const char* user = "mpi";
    const char* func = "hellompi";
    const faabric::Message& msg = faabric::util::messageFactory(user, func);
    int worldId = 1337;
    int worldSize = 2;

    // Register a rank on each
    int rankLocal = 0;
    int rankRemote = 1;

    faabric::scheduler::MpiWorldRegistry& registry =
      faabric::scheduler::getMpiWorldRegistry();
    faabric::scheduler::MpiWorld& world = registry.createWorld(msg, worldId);

    // Prepare message
    std::shared_ptr<faabric::MPIMessage> mpiMsg =
      std::make_shared<faabric::MPIMessage>();
    int bufferSize = 3;
    std::vector<int> buffer(bufferSize, 3);
    mpiMsg->set_worldid(worldId);
    mpiMsg->set_count(bufferSize);
    mpiMsg->set_buffer(buffer.data(), sizeof(int) * bufferSize);
    mpiMsg->set_sender(rankRemote);
    mpiMsg->set_destination(rankLocal);
    std::shared_ptr<faabric::scheduler::InMemoryMpiQueue> queue =
      world.getLocalQueue(rankRemote, rankLocal);

    MessageContext context;
    {
        MpiMessageEndpoint origin, destination;
        origin.start(context, SocketType::PUSH, false);
        destination.start(context, SocketType::PULL, true);

        REQUIRE_NOTHROW(origin.sendMpiMessage(mpiMsg));
        REQUIRE_NOTHROW(destination.recvMpiMessage());
    }

    REQUIRE(queue->size() == 1);
    std::shared_ptr<faabric::MPIMessage> actualMsg = queue->dequeue();
    REQUIRE(actualMsg->worldid() == worldId);
    REQUIRE(actualMsg->sender() == rankRemote);
    REQUIRE(actualMsg->destination() == rankLocal);
    context.close();
}

TEST_CASE("Test sending many MPI messages", "[transport]")
{
    cleanFaabric();
    MessageContext context;

    // Prepare MPI World
    const char* user = "mpi";
    const char* func = "hellompi";
    const faabric::Message& msg = faabric::util::messageFactory(user, func);
    int worldId = 1337;
    int worldSize = 2;

    // Register a rank on each
    int rankLocal = 0;
    int rankRemote = 1;

    faabric::scheduler::MpiWorldRegistry& registry =
      faabric::scheduler::getMpiWorldRegistry();
    faabric::scheduler::MpiWorld& world = registry.createWorld(msg, worldId);

    // Prepare message
    std::shared_ptr<faabric::MPIMessage> mpiMsg =
      std::make_shared<faabric::MPIMessage>();
    int bufferSize = 3;
    std::vector<int> buffer(bufferSize, 3);
    mpiMsg->set_worldid(worldId);
    mpiMsg->set_count(bufferSize);
    mpiMsg->set_buffer(buffer.data(), sizeof(int) * bufferSize);
    mpiMsg->set_sender(rankRemote);
    mpiMsg->set_destination(rankLocal);

    int numMessages = 100000;
    // Prepare sender
    std::thread senderThread = std::thread([&context, numMessages, mpiMsg] {
        MpiMessageEndpoint origin;
        origin.start(context, SocketType::PUSH, true);
        for (int i = 0; i < numMessages; i++) {
            mpiMsg->set_count(i);
            origin.sendMpiMessage(mpiMsg);
        }
    });

    // Prepare receiver
    std::thread receiverThread =
      std::thread([&context, numMessages, &world, mpiMsg] {
          // Set up environment
          MpiMessageEndpoint destination;
          destination.start(context, SocketType::PULL, false);
          std::shared_ptr<faabric::scheduler::InMemoryMpiQueue> queue =
            world.getLocalQueue(mpiMsg->sender(), mpiMsg->destination());

          // Handle all messages
          for (int i = 0; i < numMessages; i++) {
              destination.recvMpiMessage();
          }
          REQUIRE(queue->size() == numMessages);
          for (int i = 0; i < numMessages; i++) {
              std::shared_ptr<faabric::MPIMessage> actualMsg = queue->dequeue();
              // Just check for a couple of messages
              if (i % (numMessages / 10) == 0) {
                  REQUIRE(actualMsg->worldid() == mpiMsg->worldid());
                  REQUIRE(actualMsg->sender() == mpiMsg->sender());
                  REQUIRE(actualMsg->destination() == mpiMsg->destination());
                  // This ensures correct message ordering
                  REQUIRE(actualMsg->count() == i);
              }
          }
      });
    senderThread.join();
    receiverThread.join();

    context.close();
}

TEST_CASE("Send many messages from multiple sockets", "[transport]")
{
    cleanFaabric();
    MessageContext context;

    // Prepare MPI World
    const char* user = "mpi";
    const char* func = "hellompi";
    const faabric::Message& msg = faabric::util::messageFactory(user, func);
    int worldId = 1337;
    int worldSize = 2;

    // Register a rank on each
    int rankLocal = 0;
    int rankRemote = 1;

    faabric::scheduler::MpiWorldRegistry& registry =
      faabric::scheduler::getMpiWorldRegistry();
    faabric::scheduler::MpiWorld& world = registry.createWorld(msg, worldId);

    // Prepare message
    std::shared_ptr<faabric::MPIMessage> mpiMsg =
      std::make_shared<faabric::MPIMessage>();
    int bufferSize = 3;
    std::vector<int> buffer(bufferSize, 3);
    mpiMsg->set_worldid(worldId);
    mpiMsg->set_buffer(buffer.data(), sizeof(int) * bufferSize);
    mpiMsg->set_sender(rankRemote);
    mpiMsg->set_destination(rankLocal);

    int numSender = 5;
    int numMessages = 100000;
    // Prepare sender
    std::vector<std::thread> senderThreads;
    senderThreads.reserve(numSender);
    for (int j = 0; j < numSender; j++) {
        senderThreads.emplace_back(
          std::thread([&context, mpiMsg, numMessages, numSender] {
              MpiMessageEndpoint threadLocalOrigin;
              threadLocalOrigin.start(context, SocketType::PUSH, false);
              for (int i = 0; i < (numMessages / numSender); i++) {
                  threadLocalOrigin.sendMpiMessage(mpiMsg);
              }
          }));
    }

    // Prepare receiver
    std::thread receiverThread =
      std::thread([&context, &world, mpiMsg, numMessages, numSender] {
          std::shared_ptr<faabric::scheduler::InMemoryMpiQueue> queue =
            world.getLocalQueue(mpiMsg->sender(), mpiMsg->destination());

          MpiMessageEndpoint threadLocalDestination;
          threadLocalDestination.start(context, SocketType::PULL, true);
          for (int i = 0; i < numMessages; i++) {
              threadLocalDestination.recvMpiMessage();
          }

          REQUIRE(queue->size() == numMessages);
          for (int i = 0; i < numMessages; i++) {
              std::shared_ptr<faabric::MPIMessage> actualMsg = queue->dequeue();
              // Just check for a couple of messages
              if (i % (numMessages / 10) == 0) {
                  REQUIRE(actualMsg->worldid() == mpiMsg->worldid());
                  REQUIRE(actualMsg->sender() == mpiMsg->sender());
                  REQUIRE(actualMsg->destination() == mpiMsg->destination());
              }
          }
      });

    for (auto& t : senderThreads) {
        t.join();
    }
    receiverThread.join();

    context.close();
}
}
