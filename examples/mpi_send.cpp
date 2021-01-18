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
    int res = MPI_Init(NULL, NULL);
    if (res != MPI_SUCCESS) {
        printf("Failed on MPI init\n");
        return 1;
    }

    int rank, worldSize;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &worldSize);

    if (rank < 0) {
        printf("Rank must be positive integer or zero (is %i)\n", rank);
        return 1;
    }

    if (worldSize <= 1) {
        printf("WOrld size must be greater than 1 (is %i)\n", worldSize);
        return 1;
    }

    if (rank == 0) {
        // Send emssage tot he rest of the world
        for (int dest = 1; dest < worldSize; dest++) {
            int sentNumber = 100 + dest;
            MPI_Send(&sentNumber, 1, MPI_INT, dest, 0, MPI_COMM_WORLD);
        }

        // Wait for all responses
        int receivedNumber = 0;
        int expectedNumber;
        for (int r = 1; r < worldSize; r++) {
            expectedNumber = 100 - r;
            MPI_Recv(&receivedNumber,
                     1,
                     MPI_INT,
                     r,
                     0,
                     MPI_COMM_WORLD,
                     MPI_STATUS_IGNORE);
            if (receivedNumber != expectedNumber) {
                printf(
                  "Got unexpected number from rank %i (got %i, expected %i)\n",
                  receivedNumber,
                  expectedNumber);
                return 1;
            }
        }
    } else {
        int expectedNumber = 100 + rank;
        int sentNumber = 100 - rank;
        int receivedNumber = 0;

        // Receive message from master
        MPI_Recv(&receivedNumber, 1, MPI_INT, 0, 0, MPI_COMM_WORLD,
                 MPI_STATUS_IGNORE);
        if (receivedNumber != expectedNumber) {
            printf("Got unexpected number from master (got %i, expected %i)\n",
                   receivedNumber,
                   expectedNumber);
            return 1;
        }

        // Send response back
        MPI_Send(&sentNumber, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
    }

    MPI_Finalize();
    printf("MPI Send and Recv example finished succesfully.\n");

    return 0;
}
