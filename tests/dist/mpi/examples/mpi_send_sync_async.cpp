#include <faabric/mpi/mpi.h>
#include <stdio.h>

namespace tests::mpi {

int sendSyncAsync()
{
    MPI_Init(NULL, NULL);

    int rank;
    int worldSize;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &worldSize);

    // Send two messages to all ranks other than 0: once synchronously and once
    // asynchronously.
    if (rank == 0) {
        // Asynchronously send to the right
        MPI_Request sendRequest;
        for (int r = 1; r < worldSize; r++) {
            MPI_Isend(&r, 1, MPI_INT, r, 0, MPI_COMM_WORLD, &sendRequest);
            MPI_Send(&r, 1, MPI_INT, r, 0, MPI_COMM_WORLD);
            MPI_Wait(&sendRequest, MPI_STATUS_IGNORE);
        }
    } else {
        // Asynchronously receive twice from rank 0
        int recvValue1 = -1;
        int recvValue2 = -1;
        MPI_Request recvRequest1;
        MPI_Request recvRequest2;
        MPI_Irecv(&recvValue1, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, &recvRequest1);
        MPI_Irecv(&recvValue2, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, &recvRequest2);

        // Wait for both out of order
        MPI_Wait(&recvRequest2, MPI_STATUS_IGNORE);
        MPI_Wait(&recvRequest1, MPI_STATUS_IGNORE);

        // Check the received value is as expected
        if (recvValue1 != rank || recvValue2 != rank) {
            printf("Rank %i - async not working properly (got %i-%i expected "
                   "%i-%i)\n",
                   rank,
                   recvValue1,
                   rank,
                   recvValue2,
                   rank);
            return 1;
        }
    }
    printf("Rank %i - send sync and async working properly\n", rank);

    MPI_Finalize();

    return 0;
}
}
