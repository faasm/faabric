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

    int maxNumbers = 100;
    int actualNumbers = 30;
    auto numbers = new int[maxNumbers];

    int rank;
    int worldSize;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &worldSize);

    if (rank == 0) {
        // Send a number of values
        for (int i = 0; i < actualNumbers; i++) {
            numbers[i] = i;
        }

        MPI_Send(numbers, actualNumbers, MPI_INT, 1, 0, MPI_COMM_WORLD);
        printf("Sent %i numbers to 1\n", actualNumbers);
    } else if (rank == 1) {
        // Probe to get the length of the message
        MPI_Status statusA;
        MPI_Probe(0, 0, MPI_COMM_WORLD, &statusA);

        int probeCount;
        MPI_Get_count(&statusA, MPI_INT, &probeCount);
        if (probeCount != actualNumbers) {
            printf("Probe did not return the expected length of message "
                   "(expected %i, got %i)\n",
                   actualNumbers,
                   probeCount);
            return 1;
        }
        printf("Probe gave expected length (%i)\n", probeCount);

        // Receive the message and check
        MPI_Status statusB;
        MPI_Recv(numbers, probeCount, MPI_INT, 0, 0, MPI_COMM_WORLD, &statusB);

        for (int i = 0; i < actualNumbers; i++) {
            if (numbers[i] != i) {
                printf("Did not get expected value (expected %i but got %i)\n",
                       i,
                       numbers[i]);
                return 1;
            }
        }
    }

    delete[] numbers;

    MPI_Finalize();

    return 0;
}
