#include <faabric/scheduler/MpiWorld.h>
#include <faabric/util/logging.h>

#include <chrono>
#include <thread>

#define TARGET 1000000

typedef faabric::util::Queue<faabric::MPIMessage> MsgQueue;
typedef faabric::util::Queue<faabric::MPIMessage*> RefQueue;
typedef faabric::util::Queue<std::shared_ptr<faabric::MPIMessage>> PtrQueue;

int main()
{
    auto mq = new MsgQueue();
    auto logger = faabric::util::getLogger("u-bench");

    // Crete the message
    faabric::MPIMessage m;
    m.set_id(1337);
    m.set_worldid(1337);
    m.set_sender(1337);
    m.set_destination(1337);
    m.set_type(1337);
    std::vector<int> counts = {1, 10, 100, 1000, 2000};

    logger->info("==========");
    logger->info("FULL MSG QUEUE");
    for (auto count : counts) {
        m.set_count(count);
        std::vector<int> tmp(count, 1);
        m.set_buffer(tmp.data(), count * sizeof(int));
        
        auto t_ini = std::chrono::high_resolution_clock::now();
        std::thread th_add([&mq, &m](){
            for (int i = 0; i < TARGET; i++) {
               mq->enqueue(m);
            }
        });
        std::thread th_remove([&mq](){
            for (int i = 0; i < TARGET; i++) {
               mq->dequeue();
            }
        });
        th_add.join();
        th_remove.join();
        auto t_end = std::chrono::high_resolution_clock::now();

        auto duration = 
          std::chrono::duration_cast<std::chrono::nanoseconds>(t_end - t_ini);
        logger->info("==========");
        logger->info("Count: {}", count);
        logger->info("Throughout: {:.2f} Mops/sec", (float) TARGET / duration.count() * 1e3);
    }

    logger->info("==========");
    logger->info("PTR QUEUE");
    auto refmq = new RefQueue();
    for (auto count : counts) {
        m.set_count(count);
        std::vector<int> tmp(count, 1);
        m.set_buffer(tmp.data(), count * sizeof(int));
        
        auto t_ini = std::chrono::high_resolution_clock::now();
        std::thread th_add([&refmq, &m](){
            for (int i = 0; i < TARGET; i++) {
               refmq->enqueue(&m);
            }
        });
        std::thread th_remove([&refmq](){
            for (int i = 0; i < TARGET; i++) {
               refmq->dequeue();
            }
        });
        th_add.join();
        th_remove.join();
        auto t_end = std::chrono::high_resolution_clock::now();

        auto duration = 
          std::chrono::duration_cast<std::chrono::nanoseconds>(t_end - t_ini);
        logger->info("==========");
        logger->info("Count: {}", count);
        logger->info("Throughout: {:.2f} Mops/sec", (float) TARGET / duration.count() * 1e3);
    }

    logger->info("==========");
    logger->info("SHARED PTR QUEUE");
    auto ptrmq = new PtrQueue();
    for (auto count : counts) {
        m.set_count(count);
        std::vector<int> tmp(count, 1);
        m.set_buffer(tmp.data(), count * sizeof(int));
        auto _m = std::make_shared<faabric::MPIMessage>(std::move(m));
        
        auto t_ini = std::chrono::high_resolution_clock::now();
        std::thread th_add([&ptrmq, &_m](){
            for (int i = 0; i < TARGET; i++) {
               ptrmq->enqueue(_m);
            }
        });
        std::thread th_remove([&ptrmq](){
            for (int i = 0; i < TARGET; i++) {
               ptrmq->dequeue();
            }
        });
        th_add.join();
        th_remove.join();
        auto t_end = std::chrono::high_resolution_clock::now();

        auto duration = 
          std::chrono::duration_cast<std::chrono::nanoseconds>(t_end - t_ini);
        logger->info("==========");
        logger->info("Count: {}", count);
        logger->info("Throughout: {:.2f} Mops/sec", (float) TARGET / duration.count() * 1e3);
    }


    return 0;
}
