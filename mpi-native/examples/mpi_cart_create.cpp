#include <cmath>
#include <faabric/mpi/mpi.h>
#include <stdio.h>

#include <faabric/mpi-native/MpiExecutor.h>
#include <faabric/util/logging.h>
int main(int argc, char** argv)
{
    auto logger = faabric::util::getLogger();
    auto& scheduler = faabric::scheduler::getScheduler();
    auto& conf = faabric::util::getSystemConfig();

    bool __isRoot;
    int __worldSize;
    if (argc < 2) {
        logger->debug("Non-root process started");
        __isRoot = false;
    } else if (argc < 3) {
        logger->error("Root process started without specifying world size!");
        return 1;
    } else {
        logger->debug("Root process started");
        __worldSize = std::stoi(argv[2]);
        __isRoot = true;
        logger->debug("MPI World Size: {}", __worldSize);
    }

    // Pre-load message to bootstrap execution
    if (__isRoot) {
        faabric::Message msg = faabric::util::messageFactory("mpi", "exec");
        msg.set_mpiworldsize(__worldSize);
        scheduler.callFunction(msg);
    }

    {
        faabric::executor::SingletonPool p;
        p.startPool();
    }

    return 0;
}

int faabric::executor::mpiFunc()
{
    MPI_Init(NULL, NULL);

    int rank;
    int worldSize;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &worldSize);

    // Prepare arguments
    int ndims = 2;
    int sideLength = static_cast<int>(std::floor(std::sqrt(worldSize)));
    int nprocs = sideLength * sideLength;
    int dims[2] = { sideLength, sideLength };
    int periods[2] = { 0, 0 };
    int reorder = 0;
    MPI_Comm cart1, cart2;

    // Create two different communicators
    if (MPI_Cart_create(
          MPI_COMM_WORLD, ndims, dims, periods, reorder, &cart1) !=
        MPI_SUCCESS) {
        printf("MPI_Cart_create failed!\n");
        return 1;
    }
    if (MPI_Cart_create(
          MPI_COMM_WORLD, ndims, dims, periods, reorder, &cart2) !=
        MPI_SUCCESS) {
        printf("MPI_Cart_create failed!\n");
        return 1;
    }

    // Check that they have been allocated at different addresses. This is to
    // prevent situations in which memory is not properly allocated and we
    // end up creating an object at the base of the wasm memory array.
    if (&cart1 == &cart2) {
        printf("Both communicators allocated at the same address.\n");
        return 1;
    }
    printf("MPI_Cart_create correctly allocated.\n");

    MPI_Finalize();

    return 0;
}
