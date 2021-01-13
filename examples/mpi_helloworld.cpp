#include <faabric/util/logging.h>

#include <faabric/mpi/mpi.h>
#include <faabric/mpi-native/MpiExecutor.h>

#include <unistd.h>

int main()
{
    auto logger = faabric::util::getLogger();
    auto& scheduler = faabric::scheduler::getScheduler();

    //auto mpiFunc = _execMpiFunc;
    faabric::executor::SingletonPool p;
    p.startPool();
    
    // Send message to bootstrap execution
    faabric::Message msg = faabric::util::messageFactory("mpi", "exec");
    msg.set_mpiworldsize(1);
    logger->debug("Sending msg: {}/{}", msg.user(), msg.function());
    scheduler.callFunction(msg);
    sleep(3);

    logger->info("Hello world from Faabric MPI Main!");

    MPI_Init(NULL, NULL);

    int rank, worldSize;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &worldSize);

    logger->info("Hello faabric from process {} of {}", rank + 1, worldSize);

    MPI_Finalize();

    return 0;
}
