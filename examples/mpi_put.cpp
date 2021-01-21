#include <faabric/util/compare.h>
#include <faabric/mpi/mpi.h>
#include <stdio.h>

#define NUM_ELEMENT 4

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

    size_t elemSize = sizeof(int);

    int* sharedData;
    MPI_Alloc_mem(elemSize * NUM_ELEMENT, MPI_INFO_NULL, &sharedData);

    MPI_Win window;
    MPI_Win_create(sharedData,
                   NUM_ELEMENT * elemSize,
                   1,
                   MPI_INFO_NULL,
                   MPI_COMM_WORLD,
                   &window);
    MPI_Win_fence(0, window);

    // Put values from rank 1 to rank 0
    int putData[NUM_ELEMENT];
    if (rank == 1) {
        for (int i = 0; i < NUM_ELEMENT; i++) {
            putData[i] = 10 + i;
        }

        MPI_Put(
          putData, NUM_ELEMENT, MPI_INT, 0, 0, NUM_ELEMENT, MPI_INT, window);
    }

    MPI_Win_fence(0, window);

    // Check expectation on rank 1
    if (rank == 0) {
        int expected[NUM_ELEMENT];
        for (int i = 0; i < NUM_ELEMENT; i++) {
            expected[i] = 10 + i;
        }

        bool asExpected =
          faabric::util::compareArrays<int>(sharedData, expected, NUM_ELEMENT);
        if (!asExpected) {
            return 1;
        }
        printf("Rank %i - MPI_Put as expected\n", rank);
    }

    MPI_Win_free(&window);
    MPI_Finalize();

    return 0;
}
