#include <chrono>
#include <iostream>
#include <thread>

#include <faabric/scheduler/FunctionCallClient.h>
#include <faabric/scheduler/MpiWorld.h>
#include <faabric/scheduler/MpiWorldRegistry.h>
#include <faabric/util/logging.h>
#include <faabric/util/timing.h>

void clientTest(int numThreads, int numRpcs)
{
    auto logger = faabric::util::getLogger("bench");
    std::vector<std::thread> threadPool;
    const std::string host = "172.29.0.3";

    // Initialize message
    int bufferSize = 1;
    std::vector<int> buffer(bufferSize, 3);
    auto m = std::make_shared<faabric::MPIMessage>();
    m->set_id(1337);
    m->set_worldid(1337);
    m->set_sender(0);
    m->set_destination(1);
    m->set_type(1);
    m->set_count(bufferSize);
    m->set_buffer(buffer.data(), sizeof(int) * bufferSize);

    // Initialize clients
    for (int i = 0; i < numThreads; i++) {
        threadPool.emplace_back(std::thread([&] {
            faabric::scheduler::FunctionCallClient cli(host);
            for (int j = 0; j < numRpcs; j++) {
                PROF_START(fullRtt);
                cli.sendMPIMessage(std::make_shared<faabric::MPIMessage>());
                PROF_END(fullRtt);
            }
        }));
    }

    // Join all clients
    for (auto& th : threadPool) {
        th.join();
    }
}

int main(int argc, char* argv[])
{
    std::vector<int> numClients = {1, 2, 4, 8};
    int baseNumRpc = 8000;
    for (auto& c : numClients) {
        PROF_BEGIN
        int numRpcs = baseNumRpc / c;
        std::cout << c << " clients and " << numRpcs << " RPC/client" << std::endl;
        clientTest(c, numRpcs);
        PROF_SUMMARY
    }
    return 0;
    /*
    // Benchmark parameters
    // 1. Number of threads
    // 2. Number of loops
    int numClients = 1;
    int numRpcs = 1000;
    std::string otherHost = "172.24.0.5";

    // Create worlds
    faabric::Message msg = faabric::util::messageFactory("foo", "bar");
    msg.set_mpiworldsize(1);
    faabric::scheduler::MpiWorld& world =
      faabric::scheduler::getMpiWorldRegistry().createWorld(msg, 1337, otherHost);
    faabric::util::getLogger("bench")->info("Initialised world w/ ID: {}",
            world.getId());

    // Run benchmark
    logger->info(fmt::format("Starting RPC benchmark to {}", otherHost));
    logger->info(fmt::format("- Sending {} RPCs w/ {} clients", numRpcs, numClients));
    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
    for (uint8_t i = 0; i < numClients; i++) {
        clientTest(otherHost, numRpcs);
    }
    std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    auto duration = 
      std::chrono::duration_cast<std::chrono::milliseconds> (end - begin).count();
    logger->info(fmt::format("Sent {} RPCs in {} ms", numRpcs, duration));
    logger->info(fmt::format("- Rate: {} RPCs/sec",
                 (float) numRpcs / duration * 1e3));

    return 0;
    */
}
