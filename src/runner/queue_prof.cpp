#include <faabric/proto/faabric.pb.h>
#include <faabric/util/config.h>
#include <faabric/util/crash.h>
#include <faabric/util/environment.h>
#include <faabric/util/logging.h>
#include <faabric/util/queue.h>
#include <faabric/util/timing.h>

using namespace faabric::util;

static thread_local std::shared_ptr<Queue<std::shared_ptr<faabric::MPIMessage>>>
  queue;

static float doRun(int nProducers, int nConsumers, size_t msgSize, bool single)
{
    int nMessages = 2000000;
    int perProducer = nMessages / nProducers;
    int perConsumer = nMessages / nConsumers;

    SPDLOG_TRACE(
      "{} ({}) {} ({})", nProducers, perProducer, nConsumers, perConsumer);

    std::vector<std::shared_ptr<Queue<std::shared_ptr<faabric::MPIMessage>>>>
      queues;

    if (single) {
        queues.emplace_back(
          std::make_shared<Queue<std::shared_ptr<faabric::MPIMessage>>>());
    } else {
        for (int i = 0; i < nProducers; i++) {
            queues.emplace_back(
              std::make_shared<Queue<std::shared_ptr<faabric::MPIMessage>>>());
        }
    }

    std::vector<std::thread> producers;
    std::vector<std::thread> consumers;

    TimePoint tp = startTimer();
    for (int p = 0; p < nProducers; p++) {
        producers.emplace_back([single, p, &queues, msgSize, perProducer] {
            if (single) {
                queue = queues[0];
            } else {
                queue = queues[p];
            }

            std::vector<uint8_t> data(msgSize, 5);
            auto message = std::make_shared<faabric::MPIMessage>();
            message->set_buffer(data.data(), data.size());
            for (int i = 0; i < perProducer; i++) {
                PROF_START(Enqueue)
                queue->enqueue(message);
                PROF_END(Enqueue)
            }
        });
    }

    for (int c = 0; c < nConsumers; c++) {
        consumers.emplace_back([single, c, &queues, perConsumer] {
            if (single) {
                queue = queues[0];
            } else {
                queue = queues[c];
            }

            for (int i = 0; i < perConsumer; i++) {
                PROF_START(Dequeue)
                queue->dequeue();
                PROF_END(Dequeue)
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
    std::vector<int> producers = { 1, 10, 100 };
    std::vector<int> consumers = { 1, 10, 100 };

    SPDLOG_INFO("Starting queue profiler\n");

    std::cout.flush();

    size_t messageSize = 1024L * 1024L;

    for (auto p : producers) {
        for (auto c : consumers) {
            printf("------------- SHARED QUEUE --------------\n");
            printf("P\t C\t R\t Per msg\n");

            PROF_SUMMARY_START

            float ratio = float(p) / c;
            float runTime = doRun(p, c, messageSize, true);
            printf("%i\t %i \t %.2f \t %.5fus \n", p, c, ratio, runTime);
            std::cout.flush();

            PROF_SUMMARY_END
            PROF_CLEAR
        }
    }

    for (auto p : producers) {
        printf("------------- SPSC QUEUE --------------\n");
        printf("P\t C\t R\t Per msg\n");

        PROF_SUMMARY_START

        float runTime = doRun(p, p, messageSize, true);
        printf("%i\t %i \t %.5fus \n", p, p, runTime);
        std::cout.flush();

        PROF_SUMMARY_END
        PROF_CLEAR
    }

    return 0;
}
