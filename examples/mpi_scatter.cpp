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

    // Build an array the right size
    int nPerRank = 4;
    int n = worldSize * nPerRank;

    // Set up data in root rank
    int* allData = nullptr;
    int root = 2;
    if (rank == root) {
        allData = new int[n];
        for (int i = 0; i < n; i++) {
            allData[i] = i;
        }
    }

    // Build the expectation
    int startIdx = rank * nPerRank;
    int* expected = new int[nPerRank];
    for (int i = 0; i < nPerRank; i++) {
        expected[i] = startIdx + i;
    }

    // Do the scatter
    int* actual = new int[nPerRank];
    MPI_Scatter(allData,
                nPerRank,
                MPI_INT,
                actual,
                nPerRank,
                MPI_INT,
                root,
                MPI_COMM_WORLD);

    for (int i = 0; i < nPerRank; i++) {
        if (actual[i] != expected[i]) {
            printf("Scatter failed!\n");
            return 1;
        }
    }

    printf("Scatter %i: [%i, %i, %i, %i]\n",
           rank,
           actual[0],
           actual[1],
           actual[2],
           actual[3]);

    delete[] allData;
    delete[] actual;
    delete[] expected;

    MPI_Finalize();

    return 0;
}
