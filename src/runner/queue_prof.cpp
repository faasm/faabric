#include "faabric/util/environment.h"
#include <faabric/proto/faabric.pb.h>
#include <faabric/util/config.h>
#include <faabric/util/crash.h>
#include <faabric/util/logging.h>
#include <faabric/util/queue.h>
#include <faabric/util/timing.h>

using namespace faabric::util;

static float doRun(int nProducers, int nConsumers, size_t msgSize)
{
    int perProducer = 1000 * nConsumers;
    int nMessages = nProducers * perProducer;
    int perConsumer = 1000 * nProducers;

    SPDLOG_TRACE(
      "{} ({}) {} ({})", nProducers, perProducer, nConsumers, perConsumer);

    Queue<std::shared_ptr<faabric::MPIMessage>> queue;

    std::vector<std::thread> producers;
    std::vector<std::thread> consumers;

    TimePoint tp = startTimer();
    for (int p = 0; p < nProducers; p++) {
        producers.emplace_back([&queue, msgSize, perProducer] {
            std::vector<uint8_t> data(msgSize, 5);
            auto message = std::make_shared<faabric::MPIMessage>();
            message->set_buffer(data.data(), data.size());
            for (int i = 0; i < perProducer; i++) {
                queue.enqueue(message);
            }
        });
    }

    for (int c = 0; c < nConsumers; c++) {
        consumers.emplace_back([&queue, perConsumer] {
            for (int i = 0; i < perConsumer; i++) {
                queue.dequeue();
            }
        });
    }

    for (auto& p : producers) {
        if (p.joinable()) {
            p.join();
        }
    }

    for (auto& c : consumers) {
        if (c.joinable()) {
            c.join();
        }
    }

    long totalTime = getTimeDiffMicros(tp);
    return ((float)totalTime) / nMessages;
}

int main()
{
    std::vector<int> producers = { 1, 5, 10, 15, 20, 25, 30 };
    std::vector<int> consumers = { 1, 5, 10, 15, 20, 25, 30 };

    SPDLOG_INFO("Starting queue profiler\n");

    std::cout.flush();

    size_t messageSize = 1024L * 1024L;

    printf("Producers | Consumers | Ratio | Time per msg\n");

    for (auto p : producers) {
        for (auto c : consumers) {
            float ratio = float(p) / c;
            float runTime = doRun(p, c, messageSize);
            printf("%i\t %i \t %.2f \t %.8f \n", p, c, ratio, runTime);
            std::cout.flush();
        }
    }

    return 0;
}
