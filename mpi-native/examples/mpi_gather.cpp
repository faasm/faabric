#include <faabric/mpi/mpi.h>
#include <stdio.h>

#include <faabric/mpi-native/MpiExecutor.h>
#include <faabric/util/logging.h>

int main(int argc, char** argv)
{
    return faabric::mpi_native::mpiNativeMain(argc, argv);
}

namespace faabric::mpi_native {
int mpiFunc()
{
    MPI_Init(NULL, NULL);

    int rank;
    int worldSize;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &worldSize);

    // Build an array the right size
    int nPerRank = 4;
    int n = worldSize * nPerRank;

    // Set up containers in root
    int* expected = nullptr;
    int* actual = nullptr;
    int root = 2;
    if (rank == root) {
        expected = new int[n];
        for (int i = 0; i < n; i++) {
            expected[i] = i;
        }

        actual = new int[n];
    }

    // Build the data chunk for this rank
    int startIdx = rank * nPerRank;
    int* thisChunk = new int[nPerRank];
    for (int i = 0; i < nPerRank; i++) {
        thisChunk[i] = startIdx + i;
    }

    MPI_Gather(thisChunk,
               nPerRank,
               MPI_INT,
               actual,
               nPerRank,
               MPI_INT,
               root,
               MPI_COMM_WORLD);

    if (rank == root) {
        for (int i = 0; i < n; i++) {
            if (actual[i] != expected[i]) {
                printf("Gather failed!\n");
                return 1;
            }
        }
        printf("Gather as expected\n");
    }

    delete[] thisChunk;
    delete[] actual;
    delete[] expected;

    MPI_Finalize();

    return 0;
}
}
