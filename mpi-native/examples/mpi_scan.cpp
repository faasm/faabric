#include <faabric/mpi/mpi.h>
#include <faabric/util/compare.h>
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

    int count = 3;
    int* expected = nullptr;
    int* input = nullptr;
    int* result = nullptr;

    expected = new int[count];
    memset(expected, 0, count * sizeof(int));
    input = new int[count];
    memset(input, 0, count * sizeof(int));
    result = new int[count];
    memset(result, 0, count * sizeof(int));

    // Prepare input and expected data
    for (int i = 0; i < count; i++) {
        input[i] = rank * 10 + i;
        for (int r = 0; r <= rank; r++) {
            expected[i] += r * 10 + i;
        }
    }

    // Call the scan operation
    MPI_Scan(input, result, count, MPI_INT, MPI_SUM, MPI_COMM_WORLD);

    // Check vs. expectation
    if (!faabric::util::compareArrays<int>(result, expected, count)) {
        return 1;
    }
    printf("MPI_Scan not in-place as expected.\n");

    MPI_Barrier(MPI_COMM_WORLD);

    // Check operation in place
    MPI_Scan(input, input, count, MPI_INT, MPI_SUM, MPI_COMM_WORLD);

    // Check vs. expectation
    if (!faabric::util::compareArrays<int>(input, expected, count)) {
        return 1;
    }
    printf("MPI_Scan in-place as expected.\n");

    MPI_Finalize();

    return MPI_SUCCESS;
}
