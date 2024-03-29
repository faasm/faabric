#include <faabric/mpi/mpi.h>
#include <stdio.h>

namespace tests::mpi {

int status()
{
    MPI_Init(NULL, NULL);

    const int maxCount = 100;
    auto numbers = new int[maxCount];

    int rank;
    int worldSize;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &worldSize);

    if (rank == 0) {
        // Send a number of values
        const int actualCount = 40;
        MPI_Send(numbers, actualCount, MPI_INT, 1, 0, MPI_COMM_WORLD);
        printf("Sent %d numbers to 1\n", actualCount);
    } else if (rank == 1) {
        // Receive more than the actual count
        MPI_Status status;
        MPI_Recv(numbers, maxCount, MPI_INT, 0, 0, MPI_COMM_WORLD, &status);

        // After receiving the message, check the status to determine
        // how many numbers were actually received
        int expectedCount = 40;
        int actualCount;
        MPI_Get_count(&status, MPI_INT, &actualCount);

        if (actualCount != expectedCount) {
            printf(
              "Not expected: asked for %i values, expecting %i, but got %i\n",
              maxCount,
              expectedCount,
              actualCount);
            return 1;
        }
        printf("As expected, asked for %i values but got %i\n",
               maxCount,
               actualCount);
    }

    delete[] numbers;

    MPI_Finalize();

    return 0;
}
}
