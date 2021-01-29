#include <faabric/mpi/mpi.h>
#include <stdio.h>
#include <string.h>

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

    int rank;
    int worldSize;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &worldSize);

    // Create an array of three numbers for this process
    int numsThisProc[3] = { rank, 10 * rank, 100 * rank };
    int* expected = new int[3];
    int* result = new int[3];

    // Build expectation
    memset(expected, 0, 3 * sizeof(int));
    for (int r = 0; r < worldSize; r++) {
        expected[0] += r;
        expected[1] += 10 * r;
        expected[2] += 100 * r;
    }

    // Call allreduce
    MPI_Allreduce(numsThisProc, result, 3, MPI_INT, MPI_SUM, MPI_COMM_WORLD);

    // Check vs. expectation
    for (int i = 0; i < 3; i++) {
        if (result[i] != expected[i]) {
            printf("Allreduce failed!\n");
            return 1;
        }
    }

    printf("Rank %i: Allreduce as expected\n", rank);

    MPI_Finalize();

    return MPI_SUCCESS;
}
