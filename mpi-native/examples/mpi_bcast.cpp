#include <cstring>
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

    if (worldSize < 3) {
        printf("Need world of size 3 or more\n");
        return 1;
    }

    int root = 2;
    int expected[4] = { 0, 1, 2, 3 };
    int actual[4] = { -1, -1, -1, -1 };

    if (rank == root) {
        memcpy(actual, expected, 4 * sizeof(int));
    }

    // Broadcast (all should subsequently agree)
    MPI_Bcast(actual, 4, MPI_INT, root, MPI_COMM_WORLD);

    for (int i = 0; i < 4; i++) {
        if (actual[i] != expected[i]) {
            printf("Broadcast failed!\n");
            return 1;
        }
    }

    printf("Broadcast succeeded\n");

    MPI_Finalize();

    return 0;
}
}
