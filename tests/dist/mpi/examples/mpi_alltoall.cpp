#include <faabric/mpi/mpi.h>
#include <stdio.h>

#include <string>

namespace tests::mpi {

int allToAll()
{
    MPI_Init(NULL, NULL);

    int rank;
    int worldSize;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &worldSize);

    int chunkSize = 2;
    int fullSize = worldSize * chunkSize;

    // Arrays for sending and receiving
    int* sendBuf = new int[fullSize];
    int* expected = new int[fullSize];
    int* actual = new int[fullSize];

    // Populate data
    for (int i = 0; i < fullSize; i++) {
        // Send buffer from this rank
        sendBuf[i] = (rank * 10) + i;

        // Work out which rank this chunk of the expectation will come from
        int rankOffset = (rank * chunkSize) + (i % chunkSize);
        int recvRank = i / chunkSize;
        expected[i] = (recvRank * 10) + rankOffset;
    }

    MPI_Alltoall(
      sendBuf, chunkSize, MPI_INT, actual, chunkSize, MPI_INT, MPI_COMM_WORLD);

    int returnValue = 0;

    for (int i = 0; i < fullSize; i++) {
        if (actual[i] != expected[i]) {
            returnValue = 1;
        }
    }

    if (returnValue == 0) {
        printf("Rank %i: alltoall as expected\n", rank);
    } else {
        std::string expectedMsg = "[" + std::to_string(expected[0]);
        std::string actualMsg = "[" + std::to_string(actual[0]);
        for (int i = 1; i < fullSize; i++) {
            expectedMsg += "," + std::to_string(expected[i]);
            actualMsg += "," + std::to_string(actual[i]);
        }
        expectedMsg += "]";
        actualMsg += "]";

        printf("Rank %i: alltoall failed\n\texpected: %s\n\tgot:%s\n",
               rank,
               expectedMsg.c_str(),
               actualMsg.c_str());
    }

    delete[] sendBuf;
    delete[] actual;
    delete[] expected;

    MPI_Finalize();

    return returnValue;
}
}
