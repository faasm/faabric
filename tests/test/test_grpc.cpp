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
    // const std::string host = "172.29.0.3";
    const std::string host = "192.168.48.9";

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
    faabric::scheduler::FunctionCallClient cli(host);
    for (int i = 0; i < numThreads; i++) {
        threadPool.emplace_back(std::thread([&] {
            for (int j = 0; j < numRpcs; j++) {
                PROF_START(fullRtt)
                cli.sendMPIMessage(m);
                PROF_END(fullRtt)
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
}
