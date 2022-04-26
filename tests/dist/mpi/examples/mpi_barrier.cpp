#include <faabric/mpi/mpi.h>

#include "mpi/mpi_native.h"

namespace tests::mpi {

int barrier()
{
    MPI_Init(NULL, NULL);

    int rank;
    int worldSize;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &worldSize);

    // Call barrier multiple times. Note that there are assertions in the code
    // that check that the barrier works alright.
    int numBarriers = 5;
    for (int i = 0; i < numBarriers; i++) {
        MPI_Barrier(MPI_COMM_WORLD);
        doAllToAll(rank, worldSize, i);
    }

    MPI_Finalize();

    return 0;
}
}
