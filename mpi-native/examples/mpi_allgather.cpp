#include <faabric/mpi/mpi.h>
#include <stdio.h>

#include <faabric/mpi-native/MpiExecutor.h>
#include <faabric/util/logging.h>
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

    int rank, worldSize;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &worldSize);

    int nPerRank = 4;
    int n = worldSize * nPerRank;

    // Arrays for sending and receiving
    int* thisChunk = new int[nPerRank];
    int* expected = new int[n];
    int* actual = new int[n];

    int startIdx = rank * nPerRank;
    for (int i = 0; i < nPerRank; i++) {
        thisChunk[i] = startIdx + i;
    }

    for (int i = 0; i < n; i++) {
        expected[i] = i;
        actual[i] = -1;
    }

    MPI_Allgather(
      thisChunk, nPerRank, MPI_INT, actual, nPerRank, MPI_INT, MPI_COMM_WORLD);

    for (int i = 0; i < n; i++) {
        if (actual[i] != expected[i]) {
            printf("Allgather failed!\n");
            return 1;
        }
    }

    printf("Rank %i: gather as expected\n", rank);

    delete[] thisChunk;
    delete[] actual;
    delete[] expected;

    MPI_Finalize();

    return 0;
}
