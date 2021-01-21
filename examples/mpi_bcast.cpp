#include <cstring>
#include <faabric/mpi/mpi.h>
#include <stdio.h>

#include <faabric/util/logging.h>
#include <faabric/mpi-native/MpiExecutor.h>
int main(int argc, char** argv)
{
    auto logger = faabric::util::getLogger();
    auto& scheduler = faabric::scheduler::getScheduler();
    auto& conf = faabric::util::getSystemConfig();

    // Global configuration
    conf.maxNodes = 1;
    conf.maxNodesPerFunction = 1;

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

    if (worldSize < 3) {
        printf("Need world of size 3 or more\n");
        return 1;
    }

    int root = 2;
    int expected[4] = { 0, 1, 2, 3 };
    int actual[4] = { -1, -1, -1, -1 };

    if (rank == root) {
        memcpy(actual, expected, 4 * sizeof(int));
    }

    // Broadcast (all should subsequently agree)
    MPI_Bcast(actual, 4, MPI_INT, root, MPI_COMM_WORLD);

    for (int i = 0; i < 4; i++) {
        if (actual[i] != expected[i]) {
            printf("Broadcast failed!\n");
            return 1;
        }
    }

    printf("Broadcast succeeded\n");

    MPI_Finalize();

    return 0;
}
