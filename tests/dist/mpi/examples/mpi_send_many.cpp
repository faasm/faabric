#include <faabric/mpi/mpi.h>
#include <stdio.h>

namespace tests::mpi {

int sendMany()
{
    int res = MPI_Init(NULL, NULL);
    if (res != MPI_SUCCESS) {
        printf("Failed on MPI init\n");
        return 1;
    }

    int rank;
    int worldSize;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &worldSize);

    if (rank < 0) {
        printf("Rank must be positive integer or zero (is %i)\n", rank);
        return 1;
    }

    if (worldSize <= 1) {
        printf("World size must be greater than 1 (is %i)\n", worldSize);
        return 1;
    }

    int numMsg = 100;
    if (rank == 0) {
        for (int i = 0; i < numMsg; i++) {
            // Send message to the rest of the world
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
                    printf("Got unexpected number from rank %i (got %i, "
                           "expected %i)\n",
                           r,
                           receivedNumber,
                           expectedNumber);
                    return 1;
                }
            }
        }
    } else {
        int expectedNumber = 100 + rank;
        int sentNumber = 100 - rank;
        int receivedNumber = 0;

        // Receive message from main
        for (int i = 0; i < numMsg; i++) {
            MPI_Recv(&receivedNumber,
                     1,
                     MPI_INT,
                     0,
                     0,
                     MPI_COMM_WORLD,
                     MPI_STATUS_IGNORE);
            if (receivedNumber != expectedNumber) {
                printf(
                  "Got unexpected number from main (got %i, expected %i)\n",
                  receivedNumber,
                  expectedNumber);
                return 1;
            }

            // Send response back
            MPI_Send(&sentNumber, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
        }
    }

    MPI_Finalize();
    printf("MPI Send Many messages example finished succesfully.\n");

    return 0;
}
}
